using FluentMigrator;

namespace IsolationLevels.Migrations
{
    [Migration(3)]
    public class Listings : Migration
    {
        public override void Up()
        {
            Create.Table("listings")
                .WithColumn("id").AsInt32().Unique().PrimaryKey()
                .WithColumn("buyer").AsString().Nullable();

            Create.Table("invoices")
                .WithColumn("id").AsInt32().Unique().PrimaryKey()
                .WithColumn("listing_id").AsInt32().ForeignKey("listings", "id")
                .WithColumn("recipient").AsString().Nullable();
        }

        public override void Down()
        {
            Delete.Table("invoices");
            Delete.Table("listings");
        }
    }
}
