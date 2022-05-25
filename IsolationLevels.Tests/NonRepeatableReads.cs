using Dapper;
using System.Data;

namespace IsolationLevels.Tests;

/*
 * Неповторяемые чтения/асимметрия чтения (non-repeatable read/read skew)
 * (пример из книги М. Клеппмана "Высоконагруженные приложения")
 * Вторая транзакция переводит 100 кредитов со счета (account) Алисы 1 на счет Алисы 2 (по 500 кредитов на каждом изначально)
 * Первая транзакция читает баланс обоих счетов двумя отдельными запросами.
 * Перевод средств во второй транзакции происходит между двумя операциями чтения в первой.
 * При неправильно выбранном уровне изоляции первая транзакция может прочитать общий
 * баланс Алисы на обоих счетах как 900 кредитов (т.е. 100 кредитов где-то потеряли).
 */

public class NonRepeatableReads
{
    [SetUp]
    public void Setup()
    {
        using var connection = ConnectionFactory.GetConnection();

        connection.Execute("delete from accounts");
        connection.Execute("delete from users");

        connection.Execute("insert into users(id, name) values(1, 'Alice')");
        connection.Execute("insert into accounts(id, user_id, balance) values(1, 1, 500)");
        connection.Execute("insert into accounts(id, user_id, balance) values(2, 1, 500)");

        connection.Close();
    }

    // Уровня изоляции READ COMMITED в данном случае недостаточно, т.к. транзакция 1
    // делает первый запрос ДО начала транзакции 2, когда на первом счету 500 кредитов,
    // а второй - после её фиксации, когда на втором счету уже 400 кредитов.
    // В результате 100 кредитов куда-то пропали.
    [TestCase(IsolationLevel.ReadCommitted, ExpectedResult = 900)]

    // С уровнем изоляции READ COMMITED проблема решается: транзакция 1 читает снимок данных ДО начала данной транзакции,
    // т.е. не учитывает изменения, внесённые транзакцией 2, поэтому на первом и втором счету обнаруживает по 500 кредитов.
    [TestCase(IsolationLevel.RepeatableRead, ExpectedResult = 1000)]
    public int Test(IsolationLevel isolationLevel)
    {
        int sumBalance = 0;

        var thread1 = new Thread(() =>
        {
            using var connection = ConnectionFactory.GetConnection();
            using var tr = connection.BeginTransaction(isolationLevel);

            int balance1 = connection.QueryFirst<int>("select balance from accounts where id = 1");

            // пауза, в течение которой происходит обновление баланса на обоих счетах
            Thread.Sleep(200);

            int balance2 = connection.QueryFirst<int>("select balance from accounts where id = 2");

            sumBalance = balance1 + balance2;

            connection.Close();
        });

        var thread2 = new Thread(() =>
        {
            using var connection = ConnectionFactory.GetConnection();
            using var tr = connection.BeginTransaction(IsolationLevel.ReadCommitted);

            // Задержка, которая гарантирует, что обновление балансов произойдет после первого запроса в первой транзакции
            Thread.Sleep(100);
            connection.Execute("update accounts set balance = balance + 100 where id = 1");
            connection.Execute("update accounts set balance = balance - 100 where id = 2");

            tr.Commit();

            connection.Close();
        });

        thread1.Start(); thread2.Start();
        thread1.Join(); thread2.Join();

        return sumBalance;
    }
}