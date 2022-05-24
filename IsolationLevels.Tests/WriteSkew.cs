using Dapper;
using Npgsql;
using System.Data;

namespace IsolationLevels.Tests;

/*
 * Тест рассматривает вариант асимметрии записи.
 * (пример из книги М. Клеппмана "Высоконагруженные приложения")
 * Есть таблица doctors со столбцами name, shift, on_call.
 * Система не должна допускать ситуацию, когда на дежурстве (on call) в рамках данной смены не остаётся врачей.
 * Боб и Алиса пытаются одновременно отказаться от дежурства в смену 1234, в результате имеем две конкурирующие транзакции.
 * Правильное поведение - когда только один из врачей откажется от дежурства.
 * 
 * Добиться требуемого поведения можно правильным выбором уровня изоляции или блокировкой строк.
 * Тест-кейсы рассматривают возможные варианты.
 */
public class WriteSkew
{
    [SetUp]
    public void Setup()
    {
        using var connection = ConnectionFactory.GetConnection();

        connection.Execute("delete from doctors");

        connection.Execute("insert into doctors(name, shift_id, on_call) values('Alice', 1234, true)");
        connection.Execute("insert into doctors(name, shift_id, on_call) values('Bob', 1234, true)");

        connection.Close();
    }

    /// <summary>
    /// Тест-кейс
    /// </summary>
    /// <param name="Level">Уровень изоляции транзакций</param>
    /// <param name="Lock">Блокировка строк оператором FOR UPDATE</param>
    /// <param name="ExpectedOnCall">Ожидаемое количество дежурных врачей к завершению теста</param>
    /// <param name="SerError">Происходила ли во время теста ошибка сериализации</param>
    public record WriteSkewCase(IsolationLevel Level, bool Lock, int ExpectedOnCall, bool SerError);

    private static readonly WriteSkewCase[] WriteSkewCases = new WriteSkewCase[]
    {
        // С уровнем изоляции READ COMMITED без блокировки строк невозможно добиться правильного поведения.
        // В результате имеем ошибочный результат, когда на дежурстве не остаётся ни одного врача.
        new WriteSkewCase(IsolationLevel.ReadCommitted, Lock: false, ExpectedOnCall: 0, SerError: false),

        // Уровень изоляции REPEATABLE READ также не позволяет решить проблему, поскольку предусловие (currentlyOnCall >= 2)
        // выполняется на момент начала обеих транзакций.
        new WriteSkewCase(IsolationLevel.RepeatableRead, Lock: false, ExpectedOnCall: 0, SerError: false),

        // Уровень изоляции SERIALIZABLE решает проблему - одна из транзакций заканчивается ошибкой.
        new WriteSkewCase(IsolationLevel.Serializable, Lock: false, ExpectedOnCall: 1, SerError: true),

        // Проблему также можно решить и на уровне READ COMMITED если заблокировать строки в SELECT-запросе при помощи FOR UPDATE.
        // Конкурирующая транзакция в аналогичном SELECT-запросе будет ожидать завершения первой
        // и получит результат, который уже не будет удовлетврять предусловию currentlyOnCall >= 2, а значит,
        // завершится без ошибок.
        new WriteSkewCase(IsolationLevel.ReadCommitted, Lock: true, ExpectedOnCall: 1, SerError: false),

        // Если заблокировать строки на уровне REPEATABLE READ, то вторая транзакция при
        // попытке получить доступ к заблокированной строке будет ожидать завершения первой.
        // Если первая транзакция будет зафиксирована, то во второй произойдет ошибка сериализации.
        new WriteSkewCase(IsolationLevel.RepeatableRead, Lock: true, ExpectedOnCall: 1, SerError: true),
    };

    [TestCaseSource(nameof(WriteSkewCases))]
    public void Test(WriteSkewCase testCase)
    {
        bool withSerializationError = false;

        var thread1 = new Thread(() =>
        {
            using var connection = ConnectionFactory.GetConnection();
            string? error = UncallDoctor(connection, "Alice", testCase.Lock, testCase.Level);
            connection.Close();

            if (error == "40001")
                withSerializationError = true;
        });

        var thread2 = new Thread(() =>
        {
            using var connection = ConnectionFactory.GetConnection();
            string? error = UncallDoctor(connection, "Bob", testCase.Lock, testCase.Level);
            connection.Close();

            if (error == "40001")
                withSerializationError = true;
        });

        thread1.Start(); thread2.Start();
        thread1.Join(); thread2.Join();

        using var connection = ConnectionFactory.GetConnection();
        int currentlyOnCall = connection.Query("select * from doctors where on_call = true and shift_id = 1234").Count();
        Console.WriteLine($"Currently on call: {currentlyOnCall}");

        Assert.Multiple(() =>
        {
            Assert.That(currentlyOnCall, Is.EqualTo(testCase.ExpectedOnCall));
            Assert.That(withSerializationError, Is.EqualTo(testCase.SerError));
        });
    }

    private static string? UncallDoctor(NpgsqlConnection connection, string name, bool withLock, IsolationLevel isolationLevel)
    {
        using var tr = connection.BeginTransaction(isolationLevel);

        try
        {
            // Получаем количество врачей, дежурящих на данной смене.
            int currentlyOnCall = withLock
                ? connection.Query("select * from doctors where on_call = true and shift_id = 1234 FOR UPDATE").Count()
                : connection.Query("select * from doctors where on_call = true and shift_id = 1234").Count();

            // Ожидание, чтобы обеспечить конкуренцию транзакций
            Thread.Sleep(100);

            // Проверка бизнес-условия, в зависимости от которого принимается решение о возможности снятия врача со смены
            if (currentlyOnCall >= 2)
                connection.Execute($"update doctors set on_call = false where name = '{name}' and shift_id = 1234");

            tr.Commit();
            return null;
        }
        catch (NpgsqlException exc)
        {
            Console.WriteLine(exc.Message);
            return exc.SqlState;
        }
    }
}
