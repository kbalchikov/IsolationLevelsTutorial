using FluentMigrator;

namespace IsolationLevels.Migrations
{
    [Migration(1)]
    public class Initial : Migration
    {
        public override void Up()
        {
            Create.Table("users")
                .WithColumn("id").AsInt32().PrimaryKey().Unique()
                .WithColumn("name").AsString();

            Create.Table("accounts")
                .WithColumn("id").AsInt32().PrimaryKey().Unique()
                .WithColumn("user_id").AsInt32().ForeignKey("users", "id")
                .WithColumn("balance").AsInt32();
        }

        public override void Down()
        {
            Delete.Table("accounts");
            Delete.Table("users");
        }
    }
}
