using CsvHelper.Configuration.Attributes;

namespace Simple_ETL.Models;

public class TaxiTrip:Entity
{
    [Name("tpep_pickup_datetime")]
    public DateTime TpepPickupDatetime { get; set; }
    [Name("tpep_dropoff_datetime")]
    public DateTime TpepDropoffDatetime { get; set; }
    [Name("passenger_count")]
    public int? PassengerCount { get; set; }
    [Name("trip_distance")]
    public double TripDistance { get; set; }
    [Name("store_and_fwd_flag")]
    public string StoreAndFwdFlag { get; set; }
    [Name("PULocationID")]
    public int PULocationID { get; set; }
    [Name("DOLocationID")]
    public int DOLocationID { get; set; }
    [Name("fare_amount")]
    public double FareAmount { get; set; }
    [Name("tip_amount")]
    public double TipAmount { get; set; }

    public override string convertToCsv()
    {
        return $"{TpepPickupDatetime}, {TpepDropoffDatetime}, {PassengerCount}, {TripDistance}, {StoreAndFwdFlag}, {PULocationID}, {DOLocationID}, {FareAmount}, {TipAmount}";
    }
    
}