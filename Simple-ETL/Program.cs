using Simple_ETL.Models;

namespace Simple_ETL
{
    class Program
    {
        static void Main(string[] args)
        {
            
            FileDatabaseImporter<TaxiTrip> importer = new FileDatabaseImporter<TaxiTrip>();
            
            //Extract data
            importer.ExtractDataFromCsv("");
            
            //Transform data
            importer.RemoveDuplicates(record => $"{record.TpepPickupDatetime}-{record.TpepDropoffDatetime}-{record.PassengerCount}");
            
            var valueMapping = new Dictionary<string, string>
            {
                { "N", "No" },
                { "Y", "Yes" }
            };
            importer.UpdateColumnValues(
                record => record.StoreAndFwdFlag,
                valueMapping
            );
            importer.TrimTextFieldsInAllListItems();
            
            //Load data
            string connectionString = "Server=localhost;Database=TaxiData;Integrated Security=True;";

            string tableName = "TaxiTrips";
            importer.InsertDataIntoDB(connectionString, tableName);
            
        }
    }
}