using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Reflection;
using CsvHelper;
using Simple_ETL.Models;

namespace Simple_ETL;

public class FileDatabaseImporter<T>where T : Entity
{
    
    public List<T> Entities { get; set; }
    
    private static readonly Dictionary<Type, PropertyInfo[]> _propertiesCache = new();
    
    public FileDatabaseImporter(){}

    public void ExtractDataFromCsv(string path)
    {
        using (var reader = new StreamReader(path))
        using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
        {
            try
            {   
                Entities = csv.GetRecords<T>().ToList();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error parsing CSV: {ex.Message}");
            }
        }
    }
    
    public void TrimTextFieldsInAllListItems()
    {
        foreach (var entity in Entities)
        {
            TrimTextFields(entity);
        }
    }
    
    public void RemoveDuplicates(Func<T, string> keySelector)
    {
        var uniqueRecords = new HashSet<string>();
        var duplicates = new List<T>();

        foreach (var record in Entities)
        {
            var key = keySelector(record);
            if (uniqueRecords.Contains(key))
            {
                duplicates.Add(record);
            }
            else
            {
                uniqueRecords.Add(key);
            }
        }
    
        Entities = Entities.Except(duplicates).ToList();
        WriteDuplicatesToFile(@"..\..\..\duplicates.csv", duplicates);
    }
    
    public void UpdateColumnValues<TProperty>(Func<T, TProperty> columnSelector, Dictionary<TProperty, TProperty> valueMapping)
    {
        foreach (var record in Entities)
        {
            var property = GetCachedProperties()
                .FirstOrDefault(p => columnSelector(record).Equals(p.GetValue(record)));
        
            if (property != null && valueMapping.ContainsKey((TProperty)property.GetValue(record)))
            {
                property.SetValue(record, valueMapping[(TProperty)property.GetValue(record)]);
            }
        }
    }

    public void InsertDataIntoDB(string connectionString, string tableName)
    {
        using (var connection = new SqlConnection(connectionString))
        {
            connection.Open();
            using (var bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.DestinationTableName = tableName;
                DataTable dataTable = ConvertToDataTable(Entities);
                try
                {
                    bulkCopy.WriteToServer(dataTable);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error inserting data into database: {ex.Message}");
                }
            }
        }
    }
    
    private DataTable ConvertToDataTable(List<T> records)
    {
        var dataTable = new DataTable();
        var cachedProperties = GetCachedProperties();

        foreach (var prop in cachedProperties)
        {
            Type columnType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
            dataTable.Columns.Add(prop.Name, columnType);
        }

        foreach (var record in records)
        {
            var row = dataTable.NewRow();
            foreach (var prop in cachedProperties)
            {
                row[prop.Name] = prop.GetValue(record) ?? DBNull.Value;
            }
            dataTable.Rows.Add(row);
        }

        return dataTable;
    }
    
    private void TrimTextFields(T entity)
    {
        var cachedProperties = GetCachedProperties();

        foreach (var property in cachedProperties)
        {
            if (property.PropertyType == typeof(string))
            {
                var value = property.GetValue(entity) as string;
                if (!string.IsNullOrEmpty(value))
                {
                    property.SetValue(entity, value.Trim());
                }
            }
        }
    }
    
    private void WriteDuplicatesToFile(string path, List<T> duplicates)
    {
        using (StreamWriter writer = new StreamWriter(path, append: true))
        {

            var properties = GetCachedProperties();
            writer.WriteLine(string.Join(", ", properties.Select(p => p.Name)));
            foreach (var dublicate in duplicates)
            {
                writer.WriteLine(dublicate.convertToCsv());
            }

        }
    }
    
    private static PropertyInfo[] GetCachedProperties()
    {
        if (!_propertiesCache.ContainsKey(typeof(T)))
        {
            var properties = typeof(T).GetProperties();
            _propertiesCache[typeof(T)] = properties;
        }

        return _propertiesCache[typeof(T)];
    }
}