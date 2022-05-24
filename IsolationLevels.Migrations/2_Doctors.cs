using FluentMigrator;

namespace IsolationLevels.Migrations
{
    [Migration(2)]
    public class Doctors : Migration
    {
        public override void Up()
        {
            Create.Table("doctors")
                .WithColumn("name").AsString().PrimaryKey().Unique()
                .WithColumn("shift_id").AsInt32()
                .WithColumn("on_call").AsBoolean();
        }

        public override void Down()
        {
            Delete.Table("doctors");
        }
    }
}
