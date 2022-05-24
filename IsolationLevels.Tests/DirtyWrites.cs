using Dapper;
using Npgsql;
using System.Data;

namespace IsolationLevels.Tests;

/*
 * "Грязные" операции записи
 * (пример из книги М. Клеппмана "Высоконагруженные приложения")
 * Две операции пытаются конкурентно обновить данные.
 * Алиса и Боб пытаются одновременно купить товар с идентификатором 1234.
 * Покупка требует двух операций записи в базу данных:
 * 1. Обновления списка на сайте (listing)
 * 2. Отправка покупателю счета-фактуры (invoice)
 
 * В данном примере обновление списка и счета-фактуры Боба
 * по времени проходит между двумя аналогичными операциями Алисы.
 * 
 */
public class DirtyWrites
{
    [SetUp]
    public void Setup()
    {
        using var connection = ConnectionFactory.GetConnection();

        connection.Execute("delete from invoices");
        connection.Execute("delete from listings");

        connection.Execute("insert into listings(id, buyer) values(1234, null)");
        connection.Execute("insert into invoices(id, listing_id, recipient) values(1, 1234, null)");

        connection.Close();
    }

    /// <summary>
    /// Тест-кейс
    /// </summary>
    /// <param name="Level">Уровень изоляции</param>
    /// <param name="ListingBuyer">Ожидаемый по результату теста покупатель</param>
    /// <param name="InvoiceRecipient">Ожидаемый по результату теста адресат счета-фактуры</param>
    public record DirtyWritesCase(IsolationLevel Level, string ListingBuyer, string InvoiceRecipient);

    private static readonly DirtyWritesCase[] DirtyWritesCases = new DirtyWritesCase[]
    {
        // Пример без транзакции. В этом случае товар будет принадлежать Бобу, а счет-фактура - Алисе,
        // что является некорректным исходом.
        new DirtyWritesCase(IsolationLevel.Unspecified, "Bob", "Alice"),

        // Применение транзакции READ COMMITED. В этом случае транзакция Алисы
        // блокирует строку с товаром (listing). Когда транзакция Боба пытается обновить
        // эту строку, то обнаруживает эту блокировку и ожидает завершения транзакции Алисы.
        // После того как транзакция Алисы фиксируется, транзакция Боба продолжает работу.
        // Поэтому в конечном счете обе записи принадлежат Бобу, что является корректным исходом.
        // Таким образом для решения проблемы достаточно транзакции READ COMMITED.
        new DirtyWritesCase(IsolationLevel.ReadCommitted, "Bob", "Bob"),

        // Применение транзакции REPEATABLE READ. Транзакция Алисы блокирует строку с товаром.
        // когда транзакция с Бобом пытается обновить жту же строку, то обнарижует блокировку
        // и завершается с ошибкой сериализации (40001). Транзакция Алисы успешно фиксируется.
        // Таким образом проблема конкурентного доступа тоже решается, но с другим исходом.
        new DirtyWritesCase(IsolationLevel.RepeatableRead, "Alice", "Alice"),

        // На уровне SERIALIZABLE исход аналогичен REPEATABLE READ.
        new DirtyWritesCase(IsolationLevel.Serializable, "Alice", "Alice"),
    };

    [TestCaseSource(nameof(DirtyWritesCases))]
    public void Test(DirtyWritesCase dirtyWritesCase)
    {
        // В транзакции Алисы имитируем искусственную паузу между двумя операциями
        // обновления в 200 мс, между которыми выполняются операции обновления Боба.
        var thread1 = new Thread(() =>
        {
            Purchase(dirtyWritesCase.Level, "Alice", TimeSpan.FromMilliseconds(200));
        });

        // Транзакция Боба начинается на 100 мс позже, чтобы попасть во временной отрезок 200 мс
        // между операциями записи Алисы.
        var thread2 = new Thread(() =>
        {
            Thread.Sleep(100);
            Purchase(dirtyWritesCase.Level, "Bob", TimeSpan.Zero);
        });

        thread1.Start(); thread2.Start();
        thread1.Join(); thread2.Join();

        using var connection = ConnectionFactory.GetConnection();
        string listingBuyer = connection.QueryFirst<string>("select buyer from listings where id = 1234");
        string invoiceRecipient = connection.QueryFirst<string>("select recipient from invoices where listing_id = 1234");

        Console.WriteLine($"Listing buyer: {listingBuyer}, invoice recipient: {invoiceRecipient}");

        Assert.Multiple(() =>
        {
            Assert.That(listingBuyer, Is.EqualTo(dirtyWritesCase.ListingBuyer));
            Assert.That(invoiceRecipient, Is.EqualTo(dirtyWritesCase.InvoiceRecipient));
        });
    }

    private string? Purchase(IsolationLevel isolationLevel, string owner, TimeSpan pause)
    {
        var connection = ConnectionFactory.GetConnection();

        // Если указан уровень Unspecified, то работаем без транзакции.
        NpgsqlTransaction? transaction = null;
        if (isolationLevel != IsolationLevel.Unspecified)
            transaction = connection.BeginTransaction(isolationLevel);

        try
        {
            Console.WriteLine($"{owner} before listing updated");
            connection.Execute($"update listings set buyer = '{owner}' where id = 1234");
            Console.WriteLine($"{owner} after listing updated");

            Thread.Sleep(pause);

            connection.Execute($"update invoices set recipient = '{owner}' where listing_id = 1234");
            Console.WriteLine($"{owner} invoices updated");

            transaction?.Commit();
            Console.WriteLine($"{owner} commited");
        }
        catch (NpgsqlException exc)
        {
            Console.WriteLine($"{owner} {exc.Message}");
            return exc.SqlState;
        }
        finally
        {
            transaction?.Dispose();
            connection.Close();
        }
        return null;
    }
}
