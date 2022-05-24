using Dapper;
using System.Data;

namespace IsolationLevels.Tests;

/*
 * ������������� ������/���������� ������ (non-repeatable read/read skew)
 * (������ �� ����� �. ��������� "����������������� ����������")
 * ������ ���������� ��������� 100 �������� �� ����� (account) ����� 1 �� ���� ����� 2 (�� 500 �������� �� ������ ����������)
 * ������ ���������� ������ ������ ����� ������ ����� ���������� ���������.
 * ������� ������� �� ������ ���������� ���������� ����� ����� ���������� ������ � ������.
 * ��� ����������� ��������� ������ �������� ������ ���������� ����� ��������� �����
 * ������ ����� �� ����� ������ ��� 900 �������� (�.�. 100 �������� ���-�� ��������).
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

    // ������ �������� READ COMMITED � ������ ������ ������������, �.�. ���������� 1
    // ������ ������ ������ �� ������ ���������� 2, ����� �� ������ ����� 500 ��������,
    // � ������ - ����� � ��������, ����� �� ������ ����� ��� 400 ��������.
    // � ���������� 100 �������� ����-�� �������.
    [TestCase(IsolationLevel.ReadCommitted, ExpectedResult = 900)]

    // � ������� �������� READ COMMITED �������� ��������: ���������� 1 ������ ������ ������ �� ������ ������ ����������,
    // �.�. �� ��������� ���������, �������� ����������� 2, ������� �� ������ � ������ ����� ������������ �� 500 ��������.
    [TestCase(IsolationLevel.RepeatableRead, ExpectedResult = 1000)]
    public int Test(IsolationLevel isolationLevel)
    {
        int sumBalance = 0;

        var thread1 = new Thread(() =>
        {
            using var connection = ConnectionFactory.GetConnection();
            using var tr = connection.BeginTransaction(isolationLevel);

            int balance1 = connection.QueryFirst<int>("select balance from accounts where id = 1");

            // �����, � ������� ������� ���������� ���������� ������� �� ����� ������
            Thread.Sleep(200);

            int balance2 = connection.QueryFirst<int>("select balance from accounts where id = 2");

            sumBalance = balance1 + balance2;

            connection.Close();
        });

        var thread2 = new Thread(() =>
        {
            using var connection = ConnectionFactory.GetConnection();
            using var tr = connection.BeginTransaction(IsolationLevel.ReadCommitted);

            // ��������, ������� �����������, ��� ���������� �������� ���������� ����� ������� ������� � ������ ����������
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