using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WestPakMiddleware.Api;
using WestPakMiddleware.BusinessLogic;
using WestPakMiddleware.BusinessLogic.Access;
using WestPakMiddleware.BusinessLogic.SqlExpress;

namespace WestPakMiddleware
{
    public sealed class BusinessRules
    {
        private static readonly object locker = new object();

        //private OleDbDatabase access;
        private OdbcProvider access;
        private Rms rms;
        private SqlDatabase sql;

        private Settings settings;

        #region Instance management

        public BusinessRules()
        {
            settings = new Settings(Organization, ApplicationName, GetDefaultXmlSettings());

            var loopSecs = settings.GetAsInt32("LoopSeconds");
            var debugMode = settings.GetAsBoolean("DebugMode", true);

            var rmsUrl = settings.Get("RmsUrl");
            var rmsServerName = "Fox";

            //#if Fox
            //    rmsUrl = rmsUrl.Replace("lion", "fox");
            //    rmsServerName = "Fox";
            //#else
            //    rmsUrl = rmsUrl.Replace("fox", "lion");
            //    rmsServerName = "Lion";
            //#endif

            var rmsUsername = settings.Get("RmsUsername");
            var rmsPassword = settings.Get("RmsPassword");
            var rmsOrgName = settings.Get("RmsOrgName");

            var westPakSqlInstance = settings.Get("WestPakSqlInstance");
            var westPakSqlDbName = settings.Get("WestPakSqlDbName");
            var westPakSqlUsername = settings.Get("WestPakSqlUsername");
            var westPakSqlPassword = settings.Get("WestPakSqlPassword");

            var accessDsn = settings.Get("AccessDatabaseDsn");

            access = new OdbcProvider(accessDsn);
            rms = new Rms(rmsUsername, rmsPassword, rmsUrl, rmsOrgName, rmsServerName);
            sql = new SqlDatabase(westPakSqlInstance, westPakSqlDbName, westPakSqlUsername, westPakSqlPassword);

            SyncSecs = loopSecs;
        }

        public DateTime Initialize() {
            rms.LoadFunctionalGroupMap();
            access.TestConnection("[Pickup Sheet Detail]");

            return DateTime.Now;
        }

        public object Locker
        {
            get { return locker; }
        }

        public string RmsName
        {
            get
            {
                return rms.GetServerName();
            }
        }

        public string GetDefaultXmlSettings()
        {
            return
                "<?xml version=\"1.0\" encoding=\"utf-8\" ?>" + NewLine +
                    "<configuration>" + NewLine +
                        "<appSettings>" + NewLine +
                            "<add key=\"LoopSeconds\" value=\"" + LoopSecs + "\" />" + NewLine +
                            "<add key=\"DebugMode\" value=\"true\" />" + NewLine +
                            "<add key=\"WestPakSqlInstance\" value=\".\\sqlexpress\" />" + NewLine +
                            "<add key=\"WestPakSqlDbName\" value=\"BinTrack\" />" + NewLine +
                            "<add key=\"WestPakSqlUsername\" value=\"Fernando\" />" + NewLine +
                            "<add key=\"WestPakSqlPassword\" value=\"fee\" />" + NewLine +
                            "<add key=\"RmsUrl\" value=\"https://www.rcofox.com/Image2000/rest/\" />" + NewLine +
                            "<add key=\"RmsUsername\" value=\"berenice\" />" + NewLine +
                            "<add key=\"RmsPassword\" value=\"berenice\" />" + NewLine +
                            "<add key=\"RmsOrgName\" value=\"WestPak\" />" + NewLine +
                            "<add key=\"AccessDatabaseDsn\" value=\"GrowersBinEstimate\" />" + NewLine +
                        "</appSettings>" + NewLine +
                    "</configuration>";
        }

        #endregion

        #region Synchronization

        public bool IsRmsTicketMoreRecent(BusinessLogic.Rms.GrowerServiceTicketHeader rmsTicket, Ticket sqlTicket) {
            return !IsSqlTicketMoreRecent(sqlTicket, rmsTicket);
        }

        public bool IsSqlTicketMoreRecent(Ticket sqlTicket, BusinessLogic.Rms.GrowerServiceTicketHeader rmsTicket) {
            if (rmsTicket == null)
                return true;

            return sqlTicket.IsMoreRecentThan(rmsTicket.GetRmsCodingTimestamp());
        }

        public string FormatLastSyncDateStr(DateTime? lastSyncTimestamp)
        {
            return lastSyncTimestamp == null ? 
                    "full sync" : lastSyncTimestamp.Value.ToShortDateString() + " " + lastSyncTimestamp.Value.ToString("HH:mm:ss") + "";
        }

        public DateTime? GetEmptyLotsNextSyncTimestamp(List<Lot> lots) {
            if (lots == null || lots.Count == 0)
                return null;

            var lastSyncTimestamp = GetMaxSyncDateTime<Lot>(lots, (x) => { return x.ScheduleDate; });

            return lastSyncTimestamp.Value.AddSeconds(1);
        }

        public DateTime? GetAccessNextSyncTimestamp(List<PickUpSheet> pickUpSheets) {
            var existsAnyPickUpSheetWithModifiedDate = pickUpSheets.Exists(x => x.ModifiedDate != null);

            if (!existsAnyPickUpSheetWithModifiedDate)
                return null;

            var lastSyncTimestamp = pickUpSheets.Where(x => x.ModifiedDate != null).Max(x => x.ModifiedDate);

            return lastSyncTimestamp.Value.AddSeconds(1);
        }

        public List<Ticket> GetModifiedTicketsInSqlExpress(DateTime? syncTimestamp)
        {
            var query = sql.Query("SELECT * FROM Ticket" + (syncTimestamp != null ?
                " WHERE ModifiedDate>='" + syncTimestamp.Value.ToString("yyyy-MM-dd HH:mm:ss") + "'" : ""));

            if (query == null)
                return null;

            var result = new List<Ticket>();

            foreach (DataRow row in query.Rows)
                result.Add(Ticket.Parse(row));

            return result;
        }

        public bool IsTicketEmptyByPickUpId(string pickUpId) {
            var ticket = GetRmsTicketByPickUpId(pickUpId);

            return string.IsNullOrWhiteSpace(ticket.TicketNumber) || !ticket.ExistsChilds();
        }

        public bool ExistsTicketInRmsByPickUpId(string pickUpId) {
            var records = rms.GetRecordsUpdatedXFiltered("Grower Service Ticket Header", 100, 0, null, null, null,
                "PickupId ", ",", pickUpId, ",", null, null);

            return records != null && records.Count > 0;
        }

        public bool ExistsTicketInRms(Ticket ticket) {
            var records = rms.GetRecordsUpdatedXFiltered("Grower Service Ticket Header", 100, 0, null, null, null,
                "Ticket Number", ",", ticket.Number, ",", null, null);

            return records != null && records.Count > 0;
        }

        public BusinessLogic.Rms.GrowerServiceTicketHeader GetRmsTicketByPickUpId(string pickUpId) {
            var records = rms.GetRecordsUpdatedXFiltered("Grower Service Ticket Header", 5000, 0, null, null, null,
                "PickupId ", ",", pickUpId, ",", null, null);

            if (records == null || records.Count == 0)
                return null;

            var r = records.First();
            var result = BusinessLogic.Rms.GrowerServiceTicketHeader.Parse(r);

            var childrenNodes = rms.GetChildrenDirectoryIds(result, "Grower Service Ticket Detail");

            if (childrenNodes != null)
                foreach (var childNode in childrenNodes) {
                    var childRecordCoding = rms.GetRecordCoding(childNode);
                    result.AddChild(BusinessLogic.Rms.GrowerServiceTicketDetail.Parse(childNode, childRecordCoding));
                }

            return result;
        }


        public BusinessLogic.Rms.GrowerServiceTicketHeader GetRmsTicket(Ticket ticket)
        {
            var ticketNumber = ticket.Number;

            var records = rms.GetRecordsUpdatedXFiltered("Grower Service Ticket Header", 100, 0, null, null, null,
                "Ticket Number", ",", ticketNumber, ",", null, null);
                
            if (records == null || records.Count == 0)
                return null;

            var r = records.First();
            var result = BusinessLogic.Rms.GrowerServiceTicketHeader.Parse(r);

            var childrenNodes = rms.GetChildrenDirectoryIds(result, "Grower Service Ticket Detail");

            if (childrenNodes != null)
                foreach (var childNode in childrenNodes)
                {
                    var childRecordCoding = rms.GetRecordCoding(childNode);
                    result.AddChild(BusinessLogic.Rms.GrowerServiceTicketDetail.Parse(childNode, childRecordCoding));
                }

            return result;
        }

        public void RemoveRmsTicketChilds(BusinessLogic.Rms.GrowerServiceTicketHeader ticket)
        {
            if (ticket.Childs != null)
                foreach (var child in ticket.Childs)
                    rms.DeleteRecordIgnoreErrors(child);
        }

        public void UpdateRmsTicket(Ticket sqlTicket, BusinessLogic.Rms.GrowerServiceTicketHeader rmsTicket) {
            rms.SetGrowerServiceTicket(sqlTicket, "O");
        }

        public void CreateTicketInRms(Ticket sqlTicket)
        {
            rms.SetGrowerServiceTicket(sqlTicket, "I");
        }

        #endregion

        #region Sync dates and timestamps management

        // Sync dates management

        public bool IsFirstSyncDateTime() {
            return GetSyncDateTime() == null;
        }

        public bool IsFirstSyncDateTime(string key) {
            return GetSyncDateTime(key) == null;
        }

        public DateTime? GetSyncDateTime() {
            return settings.GetAsDateTime("LastSyncDateTime");
        }

        public DateTime? GetSyncDateTime(string key) {
            return settings.GetAsDateTime("LastSyncDateTime" + key);
        }

        public DateTime? GetMaxSyncDateTime<T>(List<T> list, Func<T, DateTime?> GetDateField) {
            var existsAnyMaxModifiedDate = list.Exists(x => GetDateField(x) != null);

            if (!existsAnyMaxModifiedDate)
                return null;

            var lastMaxDateTime = list.Where(x => GetDateField(x) != null).Max(x => GetDateField(x));

            return lastMaxDateTime.Value;
        }

        public void SetSyncDateTime(DateTime? value) {
            settings.Set("LastSyncDateTime", value);
        }

        public void SetSyncDateTime(string key, DateTime? value) {
            if (value != null)
                settings.Set("LastSyncDateTime" + key, value);
        }
        
        // RMS timestamps management

        public string GetRmsMaxTimestamp() {
            var maxTimestamp = settings.Get("MaxTimestamp");
            return !string.IsNullOrWhiteSpace(maxTimestamp) ? maxTimestamp : null;
        }

        public string GetRmsMaxTimestamp(string key) {
            var maxTimestamp = settings.Get("MaxTimestamp" + key);

            return !string.IsNullOrWhiteSpace(maxTimestamp) ?
                maxTimestamp : null;
        }

        public long GetRmsMaxTimestampAsLong(string key = "") {
            var maxTimestamp = settings.Get("MaxTimestamp" + StringOperations.Capitalize(key));
            return !string.IsNullOrWhiteSpace(maxTimestamp) ? Convert.ToInt64(maxTimestamp) : 0;
        }

        public void SetRmsMaxTimestamp(string value) {
            var currentTimestamp = GetRmsMaxTimestamp();

            if (!string.IsNullOrWhiteSpace(value) && (string.IsNullOrWhiteSpace(currentTimestamp) || Cast.ToDateTime(currentTimestamp) < Cast.ToDateTime(value)))
                settings.Set("MaxTimestamp", value);
        }

        public void SetRmsMaxTimestamp(string key, string value) {
            if (!string.IsNullOrWhiteSpace(value))
                settings.Set("MaxTimestamp" + key, value);
        }

        public void SetRmsMaxTimestampAsLong(long value) {
            SetRmsMaxTimestampAsLong("", value);
        }

        public void SetRmsMaxTimestampAsLong(string key, long value) {
            SetRmsMaxTimestamp(key, value.ToString());
        }

        #endregion

        #region Access integration

        public long lastSyncedAccessId = 0;

        public string GetAccessDatabaseDsn() {
            return settings.Get("AccessDatabaseDsn");
        }

        public long GetLastSyncAccessId() {
            return settings.GetAsInt64("LastSyncAccessId");
        }

        public void SetLastSyncAccessId(long value) {
            settings.Set("LastSyncAccessId", value);
        }

        public string FindAccessFieldById(string tableName, string fieldName, int? id) {
            if (id == null)
                return null;

            var query = access.Query("SELECT [" + fieldName + "] FROM [" + tableName + "] WHERE ID=" + id);

            if (query == null || query.Rows.Count <= 0)
                return null;

            return query.Rows[0][0] as string;
        }

        public List<PickUpSheet> GetAccessPickUpSheets(DateTime? lastSyncTimestamp) {
            var query = access.Query("SELECT P.*, G.[Grove Name] FROM [Pickup Sheet Detail] AS P, Growers AS G WHERE P.[Grove Name]=G.[Block Id] " + 
                (lastSyncTimestamp != null ? " " + "AND P.ModifiedDate>=#" + lastSyncTimestamp.Value.ToString("yyyy-MM-dd HH:mm:ss") + "#" : ""));

            if (query == null)
                return null;

            var result = new List<PickUpSheet>();

            foreach (DataRow row in query.Rows) {
                var pickUpSheet = PickUpSheet.Parse(row);
                //pickUpSheet.Area = GetAreaNameFromGrower(pickUpSheet.GroveName);
                pickUpSheet.Buyer = FindDefaultBuyerNameFromGrower(pickUpSheet.GroveName);

                result.Add(pickUpSheet);
            }

            return result;
        }

        public string FindDefaultBuyerNameFromGrower(string growerName) {
             var sqlGrowerId = FindSqlTableIdByCodeOrName("Grower", growerName);

             if (sqlGrowerId == null)
                 return null;

            var query = sql.Query("SELECT DefaultBuyerId FROM Grower WHERE Id=" + sqlGrowerId);

            if (query != null && query.Rows.Count > 0) {
                var defaultBuyerIdStr = query.Rows[0][0];

                if (defaultBuyerIdStr == null || defaultBuyerIdStr is DBNull)
                    return null;

                var defaultBuyerId = Convert.ToInt32(defaultBuyerIdStr);
                var buyerName = FindSqlTableName("Buyer", defaultBuyerId);

                return buyerName;
            }

            return null;
        }

        public string FindSqlTableName(string sqlTableName, int id) {
            var query = sql.Query("SELECT Name FROM " + sqlTableName + " WHERE Id=" + id + "");

            if (query != null && query.Rows.Count > 0)
                return query.Rows[0][0] as string;

            return null;
        }

        public int? FindSqlTableIdByCodeOrName(string sqlTableName, string name) {
            // Try to match by exact code

            var query = sql.Query("SELECT Id FROM " + sqlTableName + " WHERE Code='" + name + "'");

            if (query != null && query.Rows.Count > 0)
                return Convert.ToInt32(query.Rows[0][0]);

            // Try to match by exact name

            query = sql.Query("SELECT Id FROM " + sqlTableName + " WHERE Name='" + name + "'");

            if (query != null && query.Rows.Count > 0)
                return Convert.ToInt32(query.Rows[0][0]);

            // Try to match by name contained

            query = sql.Query("SELECT Id FROM " + sqlTableName + " WHERE Name LIKE '%" + name + "%'");

            if (query != null && query.Rows.Count > 0)
                return Convert.ToInt32(query.Rows[0][0]);

            // Try to match by first name exact match with code

            var firstName = StringOperations.GetFirstWord(name);

            if (firstName != null) {
                query = sql.Query("SELECT Id FROM " + sqlTableName + " WHERE Code='" + firstName + "'");

                if (query != null && query.Rows.Count > 0)
                    return Convert.ToInt32(query.Rows[0][0]);
            }

            return null;
        }

        public Rms.PurchaseOrder TransformToRmsPurchaseOrderPlusTicket(PickUpSheet pickUpSheet) {
            // Purchage order generation

            const int DetailsCount = 2; // 1 detail for pick-ups, 1 detail for drop-offs

            var driverRecordId = FindLocalRmsRecordId("Driver", pickUpSheet.Driver);
            var groveRecordId = FindLocalRmsRecordId("Grower", pickUpSheet.GroveName);
            var varietyRecordId = FindLocalRmsRecordId("Variety", pickUpSheet.Variety);

            var result = new Rms.PurchaseOrder();

            result.PurchaseOrderNumber = pickUpSheet.Id.ToString();
            result.Date = pickUpSheet.Date;
            result.VendorName = pickUpSheet.GroveName;
            result.VendorRecordId = groveRecordId;
            result.DropShipTo = "";
            result.Processed = "no";
            result.ProcessedDate = "";
            result.ReceiveStoreName = "";
            result.ReceiveStoreRecordId = "";
            result.ReceivedFromStoreName = "";
            result.ReceivedFromStoreRecordId = "";
            result.DestinationStoreName = "";
            result.DestinationStoreRecordId = "";
            result.Tax = "";
            result.TaxCode = "";
            result.Freight = "";
            result.Notes = "";
            result.MobileRecordId = pickUpSheet.GetMobileRecordId(rms.GetOrgNumber());
            result.DeviceId = driverRecordId;
            result.ItemType = "purchase order";

            result.Details = new List<Rms.PurchaseOrderDetail>();

            for (int i=0; i<DetailsCount; i++) {
                var mobileRecordId = result.MobileRecordId + (i == 0 ? "-pickup" : "-dropoff");

                var itemNumber = FindVarietyNumberStr(pickUpSheet.Variety, varietyRecordId);
                var description = pickUpSheet.Variety;

                if (i == 1) {
                    itemNumber = GetVarietyId(EmptiesDropOffDescription).Value.ToString();
                    description = EmptiesDropOffDescription;
                }

                result.Details.Add(new Rms.PurchaseOrderDetail {
                    MobileRecordId = mobileRecordId + "-" + i,
                    PurchaseOrder = result.PurchaseOrderNumber,
                    MobilePurchaseOrder = "",
                    ItemNumber = itemNumber,
                    Description = description,
                    UnitOfMeasure = "",
                    Quantity = ComputeRmsPurchaseOrderDetailQuantityStr(pickUpSheet, i), 
                    QuantityReceived = "",
                    QuantityLastReceived = "",
                    Cost = "",
                    Amount = "",
                    Billable = "",
                    Date = DateTime.Now,
                    CustomerJob = "",
                    LocationRecordId = groveRecordId,
                    Processed = "no",
                    ProcessedDate = "",
                    Notes = "",
                    DeviceId = driverRecordId,
                    ItemType = "purchase detail"
                });
            }

            // Ticket generation & final associations

            result.Ticket = GeneratePurchaseOrderTicket(result, pickUpSheet);
            result.Related = pickUpSheet;

            return result;
        }

        public string FindVarietyNumberStr(string variety, string varietyRecordId) {
            var query = sql.Query("SELECT Id FROM Variety WHERE Name LIKE '" + variety + "'");

            if (query != null && query.Rows.Count>0)
                return Convert.ToString(query.Rows[0][0]);

            return FindSqlTableIdStr("Product Type", Rms.ToNullableInt32(varietyRecordId), "Variety");
        }

        public int? GetVarietyId(string varietyCode) {
            return sql.GetField<int?>("SELECT Id FROM Variety WHERE Code='" + varietyCode + "'");
        }

        private string ComputeRmsPurchaseOrderDetailQuantityStr(PickUpSheet pickUpSheet, int detailRow) {
            switch (detailRow) {
                case 1:
                    if (IsValueTrue(pickUpSheet.EmptiesPickUpAll))
                        return "-1";
                    else
                        return pickUpSheet.EmptiesPickUpQty != null ? pickUpSheet.EmptiesPickUpQty.Value.ToString() : "";

                case 0:
                default:
                    return HasNoneZeroValue(pickUpSheet.Empties) ? pickUpSheet.Empties.Value.ToString() : "";
            }
        }

        public void SetRmsPurchaseOrdersPlusTickets(List<Rms.PurchaseOrder> purchaseOrders) {
            if (purchaseOrders != null)
                foreach (var po in purchaseOrders) {
                    var node = rms.SetPurchaseOrder(po);
                    rms.SetGrowerServiceTicket(po.GetPurchaseOrderBindedTicket(node));
                }
        }

        public Ticket GeneratePurchaseOrderTicket(Rms.PurchaseOrder po, PickUpSheet pickUpSheet) {
            //var ranchRecordId = FindLocalRmsRecordIdNullableInt("Ranch", pickUpSheet.Area);
            var buyerRecordId = FindLocalRmsRecordIdNullableInt("Buyer", pickUpSheet.Buyer);
            var enteredBy = FindAccessFieldById("Entered by name", "Entered by:", pickUpSheet.EnteredBy);
            var shortHauledBy = FindAccessFieldById("Short Haulers", "Hauler", pickUpSheet.ShortHauledBy);
            var longHauledBy = FindAccessFieldById("Long Haulers", "Hauler", pickUpSheet.LongHauledBy);

            var ticket = new Ticket {
                Version = 1,
                TicketType = "",
                HarvestType = "",
                Date = DateTime.Now,
                Number = "",
                ReferenceNumber = "",
                //GrossWeight = null,
                Exported = null,
                Latitude = null,
                Longitude = null,
                ModifiedDate = DateTime.Now,
                Empties = pickUpSheet.Empties ?? 0,
                ActualPickUp = pickUpSheet.ActualPickedUp ?? 0,
                ActualDrop = pickUpSheet.ActualEmptiesAtGrove ?? 0,
                //TotalEmpties = 0,
                //Timeout = null,
                Comments = "",
                //PickDateTime = null,
                PickUp = pickUpSheet.PickUp ?? 0,
                DriverProcessed = false,
                BinDumpProcessed = false,
                DriverRecordId = Rms.ToNullableInt32(po.DeviceId),
                GrowerRecordId = Rms.ToNullableInt32(po.VendorRecordId),
                RanchRecordId = Rms.ToNullableInt32(po.VendorRecordId),
                BuyerRecordId = buyerRecordId,

                PurchaseOrderRecordId = null,
                PickUpId = pickUpSheet.Id,
                EmptiesPickupAll = pickUpSheet.EmptiesPickUpAll,
                EmptiesPickupQuantity = pickUpSheet.EmptiesPickUpQty,
                AmPickUp = pickUpSheet.AmPickup,
                EnteredBy = enteredBy,
                ShortHauledBy = shortHauledBy,
                LongHauledBy = longHauledBy,
                FreskaStorage = pickUpSheet.FreskaStorage,
                Piru = pickUpSheet.Piru,
                MissedPickUp = pickUpSheet.MissedPickUp,
                RootCause = pickUpSheet.RootCause,
                MissedPickUpComment = pickUpSheet.MissedPickUpComment,

                TypeOfPick = pickUpSheet.TypeOfPick,
                CarryOver = pickUpSheet.CarryOver,

                Driver = new Driver { Name = pickUpSheet.Driver },
                Ranch = new Ranch { Name = pickUpSheet.GroveName, Grower = new Grower { Name = pickUpSheet.GroveName } },
                Vehicle = new Vehicle { Name = pickUpSheet.Truck },
                Buyer = new Buyer { Name = pickUpSheet.Buyer },
                Variety = new Variety { Name = pickUpSheet.Variety },

                TicketDetails = null
            };

            return ticket;
        }

        #endregion

        #region Record IDs management

        public void SetupMissingRecordIds() {
            SetupMissingRecordIds<Driver>("ItemType", "driver", "RecordId,First Name,Last Name", "Driver", "User", GetSqlEntity<Driver>, GetEntityFromRms<Driver>, FindRmsDriverRecordId, CreateRmsDriver);
            SetupMissingRecordIds<Grower>("ItemType,UserType", "vendor,grower", "RecordId,First Name,Last Name,Company", "Grower", "User", GetSqlEntity<Grower>, GetEntityFromRms<Grower>, FindRmsGrowerRecordId, CreateRmsGrower);
            SetupMissingRecordIds<Variety>("ItemType", "producttype", "RecordId,Name,Number", "Variety", "Product Type", GetSqlEntity<Variety>, GetEntityFromRms<Variety>, FindRmsVarietyRecordId, CreateRmsVariety);
            SetupMissingRecordIds<Ranch>("Deployment", "field", "RecordId,Store Name,Company", "Ranch", "Store", GetSqlEntity<Ranch>, GetEntityFromRms<Ranch>, FindRmsRanchRecordId, CreateRmsRanch);
            SetupMissingRecordIds<Buyer>("ItemType,UserType", "staff,buyer", "RecordId,First Name,Last Name", "Buyer", "User", GetSqlEntity<Buyer>, GetEntityFromRms<Buyer>, FindRmsBuyerRecordId, CreateRmsBuyer);

            Console.WriteLine("Done with setup steps.");
            Console.WriteLine("");
        }

        public void SetupMissingRecordIds<T>(string rmsFilterFields, string rmsFilterValues, string rmsIncludeFields, string entityName, string recordType, Func<string, List<T>> GetSqlElements, Func<string, string, string, string, List<T>> GetRmsElements, Func<T, List<T>, int?> FindRemoteRmsRecordId, Func<T, int?> CreateRmsElement = null) {
            Console.WriteLine("Configuring " + entityName + "...");

            dynamic sqlElements = GetSqlElements(entityName);

            if (sqlElements == null)
                return;

            var rmsElements = GetRmsElements(recordType, rmsFilterFields, rmsFilterValues, rmsIncludeFields);

            var unableToDiscoverRecordIdsCount = 0;

            foreach (var e in sqlElements) {
                var rmsRecordId = FindRmsRecordId(entityName, e.Id);

                if (rmsRecordId == null) {
                    rmsRecordId = FindRemoteRmsRecordId((T) e, (List<T>) rmsElements);

                    if (CreateRmsElement != null && rmsRecordId == null) {
                        Console.WriteLine("  " + entityName + " not found. Creating in RMS: " + e.ToString() + "");
                        rmsRecordId = CreateRmsElement(e);
                    }

                    if (rmsRecordId != null) {
                        if (!ExistsInsertedRmsRecordId(recordType, rmsRecordId))
                            InsertRmsRecordId(recordType, rmsRecordId, entityName, e.Id);
                    } else {
                        unableToDiscoverRecordIdsCount++;
                        Console.WriteLine("  " + unableToDiscoverRecordIdsCount + " - Unable to discover record id for item: " + e.ToString() + "");
                    }
                }
            }
        }

        public int? CreateRmsVariety(Variety v) {
            var productType = new Rms.ProductType();

            productType.Name = v.Name;
            productType.Number = v.Id;
            productType.Category = v.CommodityId != null ? GetCommodityName(v.CommodityId.Value) : "";
            productType.ItemType = "producttype";

            var result = rms.SetProductTypes(new List<Rms.ProductType> { productType });

            return Rms.ToNullableInt32(result.RecordId);
        }

        public int? CreateRmsRanch(Ranch r) {
            var store = new Rms.Store();

            store.AliasName = r.Name;
            store.StoreName = r.Name;
            store.Deployment = "field";
            store.Address1 = r.Address;
            store.City = r.City;
            store.Location = r.Name;

            var result = rms.SetStores(new List<Rms.Store>() { store });
            return Rms.ToNullableInt32(result.RecordId);
        }

        public string GetCommodityName(int id) {
            var query = sql.Query("SELECT Name FROM Commodity WHERE Id=" + id);
            return Convert.ToString(query.Rows[0][0]);
        }

        public int? CreateRmsDriver(Driver d) {
            var user = new Rms.User();

            user.FirstName = StringOperations.GetFirstWord(d.Name);
            user.LastName = StringOperations.GetLastWord(d.Name);
            user.ItemType = "driver";
            user.CompanyName = Organization;
            user.Name = d.Name;
            user.Username = user.FirstName + "." + user.LastName + "" + (DateTime.Now.Ticks / 100);
            user.Password = "changeme";
            user.UserGroup = Organization + " Drivers";
            
            var result = rms.SetUsers(new List<Rms.User> { user });

            return Rms.ToNullableInt32(result.RecordId);
        }

        public int? CreateRmsGrower(Grower g) {
            var user = new Rms.User();

            user.FirstName = StringOperations.GetFirstWord(g.Name);
            user.LastName = StringOperations.GetLastWord(g.Name);
            user.ItemType = "vendor";
            user.UserType = "grower";
            user.CompanyName = g.Company;
            user.Name = g.Name;
            user.Username = user.FirstName + "." + user.LastName + "." + (DateTime.Now.Ticks / 100);
            user.Password = "changeme";
            user.UserGroup = Organization + " Vendors";

            var result = rms.SetUsers(new List<Rms.User> { user });

            return Rms.ToNullableInt32(result.RecordId);
        }

        public int? CreateRmsBuyer(Buyer b) {
            var user = new Rms.User();

            user.FirstName = StringOperations.GetFirstWord(b.Name);
            user.LastName = StringOperations.GetLastWord(b.Name);
            user.ItemType = "staff";
            user.UserType = "buyer";
            user.CompanyName = Organization;
            user.Name = b.Name;
            user.Username = user.FirstName + "." + user.LastName + "." + (DateTime.Now.Ticks / 100);
            user.Password = "changeme";
            user.UserGroup = Organization + " Employees";

            var result = rms.SetUsers(new List<Rms.User> { user });

            return Rms.ToNullableInt32(result.RecordId);
        }

        public List<T> GetSqlEntity<T>(string tableName) {
            var query = sql.Query("SELECT * FROM " + tableName);

            if (query == null)
                return null;

            var result = new List<T>();

            foreach (DataRow row in query.Rows) {
                var parseMethod = typeof(T).GetMethod("ParseRow");
                T t = (T) parseMethod.Invoke(null, new object[] { row });

                result.Add(t);
            }

            return result;
        }

        public List<T> GetEntityFromRms<T>(string recordType, string itemType) {
            return GetEntityFromRms<T>(recordType, "ItemType", itemType, null);
        }

        public List<T> GetEntityFromRms<T>(string recordType, string itemType, string includeFields) {
            return GetEntityFromRms<T>(recordType, "ItemType", itemType, includeFields);
        }

        public List<T> GetEntityFromRms<T>(string recordType, string filterField, string filterValue, string includeFields) {
            var maxTimestamp = 0;
            var result = new List<T>();

            var records = rms.GetRecordsUpdatedXFiltered(recordType, 5000, maxTimestamp, null, null, null, filterField, ",", filterValue, ",", null, includeFields);

            if (records == null)
                return null;

            foreach (var e in records) {
                var parseMethod = typeof(T).GetMethod("ParseNode");
                T t = (T)parseMethod.Invoke(null, new object[] { e });

                result.Add(t);
            }

            return result;
        }

        public int? FindRmsDriverRecordId(Driver d, List<Driver> rmsDrivers) {
            if (rmsDrivers == null || d.Name == null)
                return null;

            // Match by full name

            foreach (var rmsDriver in rmsDrivers)
                if (rmsDriver.Name.IndexOf(d.Name, StringComparison.CurrentCultureIgnoreCase) != -1)
                    return rmsDriver.RecordId;

            // Match by first name

            foreach (var rmsDriver in rmsDrivers) {
                var driverFirstName = StringOperations.GetFirstWord(d.Name);

                if (rmsDriver.Name.IndexOf(driverFirstName, StringComparison.CurrentCultureIgnoreCase) != -1)
                    return rmsDriver.RecordId;
            }

            return null;
        }

        public int? FindRmsGrowerRecordId(Grower g, List<Grower> rmsGrowers) {
            if (rmsGrowers == null || (g.Name == null && g.Company == null && g.Code == null))
                return null;

            // Match by company name with grower name

            foreach (var rmsGrower in rmsGrowers)
                if (rmsGrower.Company != null && rmsGrower.Company.IndexOf(g.Name, StringComparison.CurrentCultureIgnoreCase) != -1)
                    return rmsGrower.RecordId;

            // Match by code

            foreach (var rmsGrower in rmsGrowers)
                if (rmsGrower.Code != null && rmsGrower.Code.IndexOf(g.Code, StringComparison.CurrentCultureIgnoreCase) != -1)
                    return rmsGrower.RecordId;

            // Match by full name

            foreach (var rmsGrower in rmsGrowers)
                if (rmsGrower.Name != null && rmsGrower.Name.IndexOf(g.Name, StringComparison.CurrentCultureIgnoreCase) != -1)
                    return rmsGrower.RecordId;

            return null;
        }

        public int? FindRmsVarietyRecordId(Variety v, List<Variety> rmsVarieties) {
            if (rmsVarieties == null || (v.Name == null && v.Code == null))
                return null;

            // Match by full name

            foreach (var rmsVariety in rmsVarieties)
                if (rmsVariety.Name != null && rmsVariety.Name.Equals(v.Name, StringComparison.CurrentCultureIgnoreCase))
                    return rmsVariety.RecordId;

            // Match by id

            foreach (var rmsVariety in rmsVarieties)
                if (rmsVariety.Id != null && rmsVariety.Id == v.Id)
                    return rmsVariety.RecordId;

            // Match by partial name

            foreach (var rmsVariety in rmsVarieties)
                if (rmsVariety.Name != null && rmsVariety.Name.IndexOf(v.Name, StringComparison.CurrentCultureIgnoreCase) != -1)
                    return rmsVariety.RecordId;

            return null;
        }

        public int? FindRmsRanchRecordId(Ranch r, List<Ranch> rmsRanches) {
            if (rmsRanches == null || (r.Name == null && r.FamousRanch == null))
                return null;

            // Match by full name

            foreach (var rmsRanch in rmsRanches)
                if (rmsRanch.Name.IndexOf(r.Name, StringComparison.CurrentCultureIgnoreCase) != -1)
                    return rmsRanch.RecordId;

            return null;
        }
        
        public int? FindRmsBuyerRecordId(Buyer b, List<Buyer> rmsBuyers) {
            if (rmsBuyers == null || (b.Code == null && b.Name == null))
                return null;

            // Match by code

            foreach (var rmsBuyer in rmsBuyers)
                if (rmsBuyer.Code != null && rmsBuyer.Code.Equals(b.Code, StringComparison.CurrentCultureIgnoreCase))
                    return rmsBuyer.RecordId;

            // Match by full name

            foreach (var rmsBuyer in rmsBuyers)
                if (rmsBuyer.Name != null && rmsBuyer.Name.IndexOf(b.Name, StringComparison.CurrentCultureIgnoreCase) != -1)
                    return rmsBuyer.RecordId;

            // Match by first name

            foreach (var rmsBuyer in rmsBuyers) {
                var driverFirstName = StringOperations.GetFirstWord(b.Name);

                if (rmsBuyer.Name != null && rmsBuyer.Name.IndexOf(driverFirstName, StringComparison.CurrentCultureIgnoreCase) != -1)
                    return rmsBuyer.RecordId;
            }

            return null;
        }

        public bool ExistsInsertedRmsRecordId(string recordType, int? recordId) {
            if (recordId == null)
                return false;

            var query = sql.Query("SELECT RmsRecordId FROM RmsRecordIds WHERE RmsRecordType='" + recordType + "' AND RmsRecordId=" + recordId);

            if (query != null && query.Rows.Count > 0)
                return true;

            return false;
        }

        public bool ExistsRmsRecordId(string tableName, int? tableId) {
            return FindRmsRecordId(tableName, tableId) != null;
        }

        public int? FindRmsRecordId(string tableName, int? tableId) {
            if (tableId == null)
                return null;

            var query = sql.Query("SELECT RmsRecordId FROM RmsRecordIds WHERE SqlTableName='" + tableName + "' AND SqlTableId=" + tableId);

            if (query != null && query.Rows.Count > 0)
                return Convert.ToInt32(query.Rows[0][0]);

            return null;
        }

        public string FindSqlTableIdStr(string recordType, int? recordId, string tableName) {
            var tableId = FindSqlTableId(recordType, recordId, tableName);
            return tableId != null ? tableId.Value.ToString() : null;
        }

        public int? FindSqlTableId(string recordType, int? recordId, string tableName) {
            if (recordType == null || recordId == null || tableName == null)
                return null;

            var query = sql.Query("SELECT SqlTableId FROM RmsRecordIds WHERE RmsRecordType='" + recordType + "' AND RmsRecordId='" + recordId + "' SqlTableName='" + tableName + "");

            if (query != null && query.Rows.Count > 0)
                return Convert.ToInt32(query.Rows[0][0]);

            return null;
        }

        public int? FindLocalRmsRecordIdNullableInt(string entityName, string value) {
            if (value == null)
                return null;
            
            return Rms.ToNullableInt32(FindLocalRmsRecordId(entityName, value));
        }

        public string FindLocalRmsRecordId(string entityName, string value) {
            return FindRmsRecordIdStr(entityName, FindSqlTableIdByCodeOrName(entityName, value));
        }

        public string FindRmsRecordIdStr(string tableName, int? tableId) {
            var result = FindRmsRecordId(tableName, tableId);
            return Convert.ToString(result);
        }

        public void InsertRmsRecordId(string rmsRecordType, int? rmsRecordId, string sqlTableName, int? sqlTableId) {
            if (rmsRecordType == null || rmsRecordId == null || sqlTableName == null || sqlTableId == null)
                throw new FormatException("rmsRecordType, rmsRecordId, sqlTableName and sqlTableId are all required fields");

            sql.Insert("RmsRecordIds",
                new List<string> { "RmsRecordType", "RmsRecordId", "SqlTableName", "SqlTableId" },
                new List<object> { rmsRecordType, rmsRecordId, sqlTableName, sqlTableId }
            );
        }

        #endregion

        #region SQL Express management

        public List<Ticket> GetSqlExpressTicketsSinceTimestamp(DateTime? syncTimestamp)
        {
            const int TicketOffset = 33;
            const int RanchOffset = TicketOffset;
            const int DriverOffset = RanchOffset+15;
            const int VehicleOffset = DriverOffset+7;
            const int BuyerOffset = VehicleOffset+5;
            const int VarietyOffset = BuyerOffset+5;

            // Get ahead all ticket details 

            var ticketDetails = GetTicketDetailsByTicketId();

            // Get all tickets and associate with the right ticket details

            var query = sql.Query("SELECT * FROM Ticket T LEFT JOIN Ranch R ON T.RanchId=R.Id LEFT JOIN Grower G ON R.GrowerId=G.Id LEFT JOIN Driver D ON T.DriverId=D.Id LEFT JOIN Vehicle V ON T.VehicleId=V.Id LEFT JOIN Buyer B ON T.BuyerId=B.Id LEFT JOIN Variety VA ON T.VarietyId=VA.Id " + (syncTimestamp != null ? " WHERE T.ModifiedDate>='" + syncTimestamp.Value.ToString("yyyy-MM-dd HH:mm:ss") + "'" : ""));

            if (query == null)
                return null;

            var result = new List<Ticket>();

            foreach (DataRow row in query.Rows)
            {
                var ticket = Ticket.Parse(row);

                if (ticket.RanchId != null)
                    ticket.Ranch = Ranch.Parse(row, RanchOffset);

                ticket.Driver = Driver.Parse(row, DriverOffset);

                if (ticket.VehicleId != null)
                    ticket.Vehicle = Vehicle.Parse(row, VehicleOffset);

                if (ticket.BuyerId != null)
                    ticket.Buyer = Buyer.Parse(row, BuyerOffset);

                if (ticket.VarietyId != null)
                    ticket.Variety = Variety.Parse(row, VarietyOffset);
                    
                if (ticketDetails.ContainsKey(ticket.Id))
                    ticket.TicketDetails = ticketDetails[ticket.Id];

                result.Add(ticket);
            }

            return result;
        }

        private Dictionary<Guid, List<TicketDetail>> GetTicketDetailsByTicketId()
        {
            var query = sql.Query("SELECT D.*, B.* FROM TicketDetail D, Bin B WHERE D.BinId=B.Id");
            
            if (query == null)
                return null;

            var result = new Dictionary<Guid, List<TicketDetail>>();

            foreach (DataRow row in query.Rows)
            {
                if (row[3] is DBNull)
                    continue;

                var ticketId = (Guid) row[3];
                var ticketDetails = new List<TicketDetail>();

                if (result.ContainsKey(ticketId))
                {
                    ticketDetails = result[ticketId];
                    ticketDetails.Add(TicketDetail.Parse(row));
                }
                else
                {
                    ticketDetails.Add(TicketDetail.Parse(row));
                    result.Add(ticketId, ticketDetails);
                }
            }

            return result;
        }

        public Ticket GetSqlTicket(string ticketNumber) {
            var query = sql.Query("SELECT * FROM Ticket WHERE Number='" + ticketNumber + "'");

            if (query == null || query.Rows.Count == 0)
                return null;

            return Ticket.Parse(query.Rows[0]);
        }

        public void UpdateSqlTicket(BusinessLogic.Rms.GrowerServiceTicketHeader t) {
            int? driverId = FindEntityId("Driver", t.GetDriverName());
            int? ranchId = FindEntityId("Ranch", t.Grower);
            int? buyerId = FindEntityId("Buyer", t.GetBuyerName());
            int? varietyId = FindEntityId("Variety", t.Variety);
            int? vehicleId = FindEntityId("Vehicle", t.TruckNumber.ToString());

            bool? binDumpProcessed = null; //Rms.ToBooleanOrNull(t.BinDumpProcessed);
            DateTime? timeOut = null; // t.TimeOut;
            decimal? grossWeight = null;

            UpdateSqlTicket(t.TicketNumber, driverId.Value, ranchId, buyerId, varietyId, t.ReferenceNumber, vehicleId, grossWeight, t.Latitude, t.Longitude, t.Empties, t.ActualPickUp, t.ActualDrop, t.TotalEmpties, Rms.ToDateTime(t.TimeOut), t.Comments, t.PickDateTime, t.PickUp, Rms.ToBooleanOrNull(t.Processed), binDumpProcessed, (int?) t.DriverRecordId, (int?) t.GrowerRecordId, (int?) t.RanchRecordId, (int?) t.BuyerRecordId, t.Status, t.DriverCreated);

            if (t.Childs == null || t.Childs.Count == 0)
                return;

            // Update ticket details

            var sqlTicket = GetSqlTicket(t.TicketNumber);
            DeleteTicketDetails(sqlTicket.Id);

            foreach (var d in t.Childs) {
                var binId = FindBinId(d.BinNumber);
                CreateSqlTicketDetail(d.GetBinLevel(), binId.Value, sqlTicket.Id, d.Status, d.Weight);
            }
        }

        private void DeleteTicketDetails(Guid ticketId) {
            sql.Delete("TicketDetail", "TicketId='" + ticketId.ToString() + "'");
        }
            
        public void UpdateSqlTicket(string number, int driverId, int? ranchId, int? buyerId, int? varietyId, string referenceNumber, int? vehicleId, decimal? grossWeight, float? latitude, float? longitude, int? empties, int? actualPickUp, int? actualDrop, int? totalEmpties, DateTime? timeout, string comments, DateTime? pickDateTime, int? pickUp, bool? driverProcessed, bool? binDumpProcessed, int? driverRecordId, int? growerRecordId, int? ranchRecordId, int? buyerRecordId, string status, bool driverCreated) {
            sql.Update("Ticket",
                new List<string> { 
                    "DriverId", "RanchId", "BuyerId", "VarietyId", "ReferenceNumber", "VehicleId", "GrossWeight", 
                    "Latitude", "Longitude", "Empties", "ActualPickUp", "ActualDrop", "TotalEmpties", "Timeout", 
                    "Comments", "PickDateTime", "PickUp", "DriverProcessed", "BinDumpProcessed", "DriverRecordId", 
                    "GrowerRecordId", "RanchRecordId", "BuyerRecordId", "Status", "DriverCreated" 
                },
                new List<object> { 
                    driverId, ranchId, buyerId, varietyId, referenceNumber, vehicleId, grossWeight, 
                    latitude, longitude, empties, actualPickUp, actualDrop, totalEmpties, timeout, 
                    comments, pickDateTime, pickUp, driverProcessed, binDumpProcessed, driverRecordId, 
                    growerRecordId, ranchRecordId, buyerRecordId, status, driverCreated
                },
                "Number='" + number + "'"
            );
        }

        public int? FindEntityId(string tableName, string name) {
            if (string.IsNullOrWhiteSpace(name))
                return null;

            var field = sql.GetField<int?>("SELECT Id FROM " + tableName + " WHERE Name='" + name + "'");

            if (field != null)
                return field;

            field = sql.GetField<int?>("SELECT Id FROM " + tableName + " WHERE Name LIKE '" + name + "'");

            if (field != null)
                return field;

            field = sql.GetField<int?>("SELECT Id FROM " + tableName + " WHERE Name LIKE '" + name + "%'");

            if (field != null)
                return field;

            return null;
        }

        public void CreateSqlTicket(BusinessLogic.Rms.GrowerServiceTicketHeader t) {
            int? driverId = FindEntityId("Driver", t.GetDriverName());
            int? ranchId = FindEntityId("Ranch", t.Grower);
            int? buyerId = FindEntityId("Buyer", t.GetBuyerName());
            int? varietyId = FindEntityId("Variety", t.Variety);
            int? vehicleId = FindEntityId("Vehicle", t.TruckNumber.ToString());
            bool? binDumpProcessed = null;
            DateTime? timeOut = null; // t.TimeOut;
            
            CreateSqlTicket(1, t.GetTicketType(), t.GetHarvestType(), t.TicketNumber, t.GetDateTime(), driverId.Value, ranchId, buyerId, varietyId, t.ReferenceNumber,
                vehicleId, null, null, (float?) t.Latitude, (float?) t.Longitude, t.GetRmsCodingTimestamp(), t.Empties, t.ActualPickUp, t.ActualDrop, t.TotalEmpties, timeOut, t.Comments, t.PickDateTime, t.PickUp, Rms.ToBoolean(t.Processed), binDumpProcessed, (int?) t.DriverRecordId, (int?) t.GrowerRecordId, (int?) t.RanchRecordId, (int?) t.BuyerRecordId, t.Status, t.DriverCreated);

            if (t.Childs == null || t.Childs.Count == 0)
                return;

            var sqlTicket = GetSqlTicket(t.TicketNumber);

            foreach (var d in t.Childs) {
                var binId = FindBinId(d.BinNumber);
                CreateSqlTicketDetail(d.GetBinLevel(), binId.Value, sqlTicket.Id, d.Status, d.Weight);
            }
        }

        public Guid? FindBinId(string binNumber) {
            var binId = sql.GetField<Guid?>("SELECT Id FROM Bin WHERE Number='" + binNumber + "'");

            if (binId == null)
                binId = sql.GetField<Guid?>("SELECT Id FROM Bin WHERE Number='" + DefaultBinNumber + "'");

            return binId;
        }

        public void CreateSqlTicket(int version, string ticketType, string harvestType, string number, DateTime? date, int driverId, int? ranchId, int? buyerId, int? varietyId, string referenceNumber, int? vehicleId, decimal? grossWeight, bool? exported, float? latitude, float? longitude, DateTime? modifiedDate, int? empties, int? actualPickUp, int? actualDrop, int? totalEmpties, DateTime? timeout, string comments, DateTime? pickDateTime, int? pickUp, bool? driverProcessed, bool? binDumpProcessed, int? driverRecordId, int? growerRecordId, int? ranchRecordId, int? buyerRecordId, string status, bool driverCreated) {
            sql.Insert("Ticket",
                new List<string> { 
                    "Id", "Version", "TicketType", "HarvestType", "Number", "Date", "DriverId", "RanchId", "BuyerId", "VarietyId", "ReferenceNumber", "VehicleId", 
                    "GrossWeight", "Exported", "Latitude", "Longitude", "Empties", "ActualPickUp", "ActualDrop", "TotalEmpties", "Timeout", "Comments", "PickDateTime", 
                    "PickUp", "DriverProcessed", "BinDumpProcessed", "DriverRecordId", "GrowerRecordId", "RanchRecordId", "BuyerRecordId", "Status", "DriverCreated" 
                },
                new List<object> { 
                    Guid.NewGuid(), version, ticketType, harvestType, number, date, driverId, ranchId, buyerId, varietyId, referenceNumber, vehicleId, 
                    grossWeight, exported, latitude, longitude, empties, actualPickUp, actualDrop, totalEmpties, timeout, comments, pickDateTime, 
                    pickUp, driverProcessed, binDumpProcessed, driverRecordId, growerRecordId, ranchRecordId, buyerRecordId, status, driverCreated 
                }
            );
        }

        public void CreateSqlTicketDetail(string binLevel, Guid binId, Guid ticketId, string status, decimal? weight) {
            sql.Insert("TicketDetail",
                new List<string> { "Id", "BinLevel", "BinId", "TicketId", "Status", "Weight" },
                new List<object> { Guid.NewGuid(), binLevel, binId, ticketId, status, weight }
            );
        }

        public Ticket ConvertToSqlTicket(BusinessLogic.Rms.GrowerServiceTicketHeader rmsTicket) {
            var result = new Ticket();

            result.Version = 1;
            result.TicketType = "RanchPickup";
            result.HarvestType = "Regular";
            result.Number = rmsTicket.TicketNumber;
            result.Date = rmsTicket.DateTime != null ? rmsTicket.DateTime.Value : DateTime.MinValue;
            //result.DriverId = rmsTicket.DriverFirstName
            //result.RanchId = rmsTicket.Ranch
            //result.BuyerId = rmsTicket.BuyerFirstName
            //result.VarietyId = rmsTicket.Variety
            result.ReferenceNumber = rmsTicket.ReferenceNumber;
            //result.VehicleId = rmsTicket.TruckNumber
            //result.GrossWeight = rmsTicket
            result.Latitude = rmsTicket.Latitude;
            result.Longitude = rmsTicket.Longitude;
            result.ModifiedDate = rmsTicket.GetRmsCodingTimestamp();
            result.Empties = rmsTicket.Empties ?? 0;
            result.ActualPickUp = rmsTicket.ActualPickUp ?? 0;
            result.ActualDrop = rmsTicket.ActualDrop ?? 0;
            result.TotalEmpties = rmsTicket.TotalEmpties ?? 0;
            //result.Timeout = rmsTicket.TimeOut;
            result.Comments = rmsTicket.Comments;
            result.PickDateTime = rmsTicket.PickDateTime;
            //result.PickUp = rmsTicket.PickUp;
            result.DriverProcessed = Rms.ToBoolean(rmsTicket.Processed);
            //result.BinDumpProcessed = rmsTicket.
            result.DriverRecordId = (int)rmsTicket.DriverRecordId;
            result.GrowerRecordId = (int)rmsTicket.GrowerRecordId;
            result.RanchRecordId = (int)rmsTicket.RanchRecordId;
            result.BuyerRecordId = (int)rmsTicket.BuyerRecordId;

            return result;
        }

        public string FindSqlEntityName(string tableName, int? id) {
            if (id == null)
                return null;

            return sql.GetField<string>("SELECT Name FROM " + tableName + (id == null ? "" : " WHERE id=" + id));
        }

        #endregion

        #region Lots

        public bool IsEmptyLot(Guid? lotId) {
            return !sql.Exists("SELECT * FROM LotBin WHERE LotId='" + lotId + "'");
        }

        public bool ExistsSqlLot(Lot l) {
            if (l == null || string.IsNullOrWhiteSpace(l.LotNumber))
                return false;

            return sql.Exists("SELECT * FROM Lot WHERE LotNumber='" + l.LotNumber + "'");
        }

        public bool ExistsRmsLot(Lot l) {
            var records = rms.GetRecordsUpdatedXFiltered("Lot Header", 5000, 0,
                null, null, null, "LotNumber ", ",", l.LotNumber, ",", null, null);

            return records != null && records.Count > 0;
        }

        public bool ExistsSqlLotTicket(string lotNumber, string ticketNumber) {
            var lotId = FindSqlLotId(lotNumber);
            var ticketId = FindTicketId(ticketNumber);

            if (lotId == null || ticketId == null)
                return false;

            return sql.Exists("SELECT * FROM LotBin WHERE LotId='" + lotId + "' AND TicketId='" + ticketId + "'");
        }

        public List<Lot> GetNewSqlEmptyLots(DateTime? scheduleDate) {
            var query = sql.Query("SELECT * FROM Lot WHERE DumpStatus='Not Started' AND Id NOT IN (SELECT DISTINCT LotId FROM LotBin)" + 
                (scheduleDate != null && scheduleDate != DateTime.MinValue ? " AND ScheduleDate>'" + scheduleDate.Value.ToString("yyyy-MM-dd HH:mm:ss") + "'" : ""));

            if (query == null)
                return null;

            var result = new List<Lot>();

            foreach (DataRow row in query.Rows)
                result.Add(Lot.Parse(row));

            if (result == null)
                return null;

            var lots = new List<Lot>();

            foreach (var l in result)
                if (!ExistsRmsLot(l)) // We don't want to override existing empty tickets in RMS and with that clear data
                    lots.Add(l);

            return lots;
        }

        public void SetRmsEmptyLots(List<Lot> lots) {
            if (lots == null)
                return;

            rms.SetLots(lots);
        }

        public void UpdateSqlLotBins(Lot l) {
            if (l == null || l.LotBins == null)
                return;

            var lotId = FindSqlLotId(l.LotNumber);

            if (lotId == null)
                return;

            // In RMS each lot detail (LotBin entry) represents a all ticket with multiple bins

            foreach (var lotBin in l.LotBins) {
                if (ExistsSqlLotTicket(l.LotNumber, lotBin.TicketNumber))
                    DeleteLotBins(l.LotNumber, lotBin.TicketNumber);

                var binNumbers = lotBin.GetBinNumbers();

                if (binNumbers == null || binNumbers.Count == 0)
                    continue;

                var ticketId = FindTicketId(lotBin.TicketNumber);

                foreach (var binNumber in binNumbers) {
                    if (string.IsNullOrWhiteSpace(binNumber))
                        continue;

                    var binId = FindBinId(binNumber);
                    var weight = lotBin.Weight ?? 0;

                    sql.Insert("LotBin",
                        new List<string> { "Id", "Weight", "LotId", "TicketId", "BinId" },
                        new List<object> { Guid.NewGuid(), weight, lotId, ticketId, binId }
                    );
                }
            }

            // Update lot header information as well

            sql.Update("Lot",
                new List<string> { "DumpStatus", "StartedDumpingDateTime", "FinishedDumpingDateTime" },
                new List<object> { l.DumpStatus, l.StartedDumpingDateTime, l.FinishedDumpingDateTime },
                "LotNumber='" + l.LotNumber + "'"
            );
        }

        public void DeleteLotBins(string lotNumber, string ticketNumber) {
            var lotId = FindSqlLotId(lotNumber);
            var ticketId = FindTicketId(ticketNumber);

            if (lotId == null || ticketId == null)
                return;

            sql.Delete("LotBin", "LotId='" + lotId + "' AND TicketId='" + ticketId + "'");
        }

        public Guid? FindTicketId(string ticketNumber) {
            return sql.GetField<Guid?>("SELECT * FROM Ticket WHERE Number='" + ticketNumber + "'");
        }

        public Guid? FindSqlLotId(string lotNumber) {
            return sql.GetField<Guid?>("SELECT * FROM Lot WHERE LotNumber='" + lotNumber + "'");
        }

        #endregion

        #region RMS management

        public Rms.Node TestRmsConnection()
        {
            return rms.TestRmsConnection();
        }

        public List<Driver> GetDriversFromRms() {
            var maxTimestamp = 0;
            var result = new List<Driver>();

            var records = rms.GetRecordsUpdatedXFiltered("User", 100, maxTimestamp, null, null, null, "ItemType", ",", "driver", ",", null, null);

            if (records == null)
                return null;

            foreach (var e in records)
                result.Add(Driver.ParseNode(e));

            return result;
        }

        public List<BusinessLogic.Rms.GrowerServiceTicketHeader> GetRmsTicketsSinceTimestamp(long maxTimestamp = 0) {
            var result = new List<BusinessLogic.Rms.GrowerServiceTicketHeader>();

            var records = rms.GetRecordsUpdatedXFiltered("Grower Service Ticket Header", 10000, maxTimestamp,
                null, null, null, null, ",", null, ",", null, null);

            if (records == null || records.Count == 0)
                return null;

            foreach (var r in records) {
                var rmsTicket = BusinessLogic.Rms.GrowerServiceTicketHeader.Parse(r);

                if (!rmsTicket.ExistsTicketNumber())
                    continue;

                var childrenNodes = rms.GetChildrenDirectoryIds(r, "Grower Service Ticket Detail");

                if (childrenNodes != null)
                    foreach (var childNode in childrenNodes) {
                        var childRecordCoding = rms.GetRecordCoding(childNode);
                        rmsTicket.AddChild(BusinessLogic.Rms.GrowerServiceTicketDetail.Parse(childNode, childRecordCoding));
                    }

                result.Add(rmsTicket);
            }

            return result;
        }

        public List<Lot> GetRmsLotsWithLotBinsSinceTimestamp(long maxTimestamp = 0) {
            var result = new List<Lot>();

            var records = rms.GetRecordsUpdatedXFiltered("Lot Header", 5000, maxTimestamp,
                null, null, null, "", ",", "", ",", null, null);

            if (records == null || records.Count == 0)
                return null;

            records = records.Where(x => !string.Equals(x.GetCodingField("DumpStatus"), "NotStarted", StringComparison.CurrentCultureIgnoreCase) &&
                !string.Equals(x.GetCodingField("DumpStatus"), "Not Started", StringComparison.CurrentCultureIgnoreCase)).ToList();

            var counter = 0;

            foreach (var r in records) {
                var rmsLot = Lot.Parse(r);
                Console.WriteLine("   " + records.Count + "/" + (counter++) + ". Filtering lot " + rmsLot.ToString());

                var childrenNodes = rms.GetChildrenDirectoryIds(r, "Lot Detail");

                if (childrenNodes != null)
                    foreach (var childNode in childrenNodes) {
                        var childRecordCoding = rms.GetRecordCoding(childNode);
                        rmsLot.AddLotBins(LotBin.Parse(childNode, childRecordCoding));
                    }

                if (!rmsLot.ExistsLotBins())
                    continue;

                result.Add(rmsLot);

                if (counter == 3)
                    break;
            }

            return result;
        }

        public List<Ticket> GetTicketsFromRms()
        {
            var maxTimestamp = 0;
            var result = new List<Ticket>();

            var records = rms.GetRecordsUpdatedXFiltered("Grower Service Ticket Header", 100, maxTimestamp, 
                null, null, null, null, ",", null, ",", null, null);

            if (records != null)
                foreach (var e in records)
                {
                    result.Add(new Ticket { 
                        
                    });
                }

            return result;
        }

        public List<Rms.Node> GetGrowerServiceTicketDetails()
        {
            var recordDisplayType = "Grower Service Ticket Detail";
            var maxNumberfullDataRecords = 500;
            var maxTimestamp = 0;
            var fromDate = DateTime.MinValue;
            var toDate = DateTime.MinValue;
            var filterFields = "Year,Month";
            var strFieldDelim = ",";
            var filterValues = "2017,01";
            var strValueDelim = ",";
            var includeFields = "Ticket Number,Bin Number";

            var records = rms.GetRecordsUpdatedXFiltered(recordDisplayType, maxNumberfullDataRecords, maxTimestamp, 
                null, null, null, filterFields, strFieldDelim, filterValues, strValueDelim, null, includeFields);

            return records;
        }

        public void CreatePurchaseOrderInRms(PickUpSheet pickUpSheet) {
            var result = new List<Rms.PurchaseOrder>();

            rms.SetPurchaseOrders(result);
        }

        #endregion

        #region Elapsed time management

        private DateTime startTime;
        private DateTime endTime;

        public int SyncSecs {
            get;
            set;
        }

        public static int SecMillis {
            get {
                return 1000;
            }
        }

        public void WriteCronStartLine(string msg)
        {
            Console.WriteLine(msg);
            StartCron();
        }

        public void WriteCronEndLine(string msg)
        {
            StopCron();

            var elapsedSecs = GetCronElapsedSecs();

            Console.WriteLine(msg, elapsedSecs);
        }

        public void StartCron()
        {
            startTime = DateTime.Now;
        }

        public void StopCron()
        {
            endTime = DateTime.Now;
        }

        public int GetCronElapsedSecs()
        {
            return new TimeSpan(endTime.Ticks - startTime.Ticks).Seconds;
        }

        #endregion

        #region Logging

        #endregion

        #region General rules

        public static bool HasNoneZeroValue(int? value) {
            return value != null && value > 0;
        }

        public static bool IsValueTrue(bool? value) {
            return value != null && value == true;
        }

        #endregion

        #region Constants

        public const string Organization = "WestPak";

        public const string ApplicationName = "Middleware";

        public const string EmptiesDropOffDescription = "Empties Drop Off";

        public const string EmptiesPickUpDescription = "Empties Pick Up";

        public const int DefaultBinNumber = 999999;

        private const int LoopSecs = 10;

        private static string NewLine = Environment.NewLine;

        #endregion

        #region Temp

        //public List<Ticket> GetModifiedTicketsInRms(DateTime? syncTimestamp)
        //{

        //}

        // SQL -> RMS sync

        public List<Ticket> GetRmsTicketsSinceTimestampOld(DateTime? syncTimestamp) {
            var maxTimestamp = syncTimestamp == null ? 0 : syncTimestamp.Value.Ticks / 1000;
            var result = new List<Ticket>();

            var records = rms.GetRecordsUpdatedXFiltered("Grower Service Ticket Header", 100, maxTimestamp,
                null, null, null, null, ",", null, ",", null, null);

            if (records == null || records.Count == 0)
                return null;

            foreach (var r in records) {
                var rmsTicket = BusinessLogic.Rms.GrowerServiceTicketHeader.Parse(r);

                var childrenNodes = rms.GetChildrenDirectoryIds(r, "Grower Service Ticket Detail");

                if (childrenNodes != null)
                    foreach (var childNode in childrenNodes) {
                        var childRecordCoding = rms.GetRecordCoding(childNode);
                        rmsTicket.AddChild(BusinessLogic.Rms.GrowerServiceTicketDetail.Parse(childNode, childRecordCoding));
                    }

                result.Add(ConvertToSqlTicket(rmsTicket));
            }

            return result;
        }

        public bool existsNewSqlTickets(List<Ticket> sqlTickets, List<Rms.Node> rmsTickets) {
            return false;
        }

        public bool existsUpdatedSqlTickets(List<Ticket> sqlTickets, List<Rms.Node> rmsTickets) {
            return false;
        }

        public bool existsRemovedSqlTicketsInRms(List<Ticket> sqlTickets, List<Rms.Node> rmsTickets) {
            return false;
        }

        // RMS -> SQL sync

        public bool existsNewRmsTickets(List<Ticket> sqlTickets, List<Rms.Node> rmsTickets) {
            return false;
        }

        public bool existsUpdatedRmsTickets(List<Ticket> sqlTickets, List<Rms.Node> rmsTickets) {
            return false;
        }

        public bool existsRemovedRmsTicketsInSql(List<Ticket> sqlTickets, List<Rms.Node> rmsTickets) {
            return false;
        }

        #endregion
    }
}
