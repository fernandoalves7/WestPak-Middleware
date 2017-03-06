using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System.Runtime.Serialization;
using System.IO;
using WestPakMiddleware.BusinessLogic.SqlExpress;

namespace WestPakMiddleware.Api {
    public sealed class Rms {
        private const int LibraryStoreNumber = -1;
        private const string LibraryStoreName = "Library";

        private readonly string credentialsString;
        private readonly string orgName;
        private readonly string url;
        private readonly string serverName;

        private Dictionary<string, string> functionalGroupMap;

        // Instantiation

        public Rms(string username, string password, string url, string orgName, string serverName) {
            this.url = url;
            this.orgName = orgName;
            this.serverName = serverName;

            credentialsString = username + "/" + password;

            functionalGroupMap = null;
        }

        public Node TestRmsConnection()
        {
            Node node = null;

            //var thread = new Thread(() => {
            try
            {
                var json = InvokeJson<JObject>("securityservice", "getUserInfo", "");

                node = new Node
                {
                    ObjectId = Convert.ToInt32((string)json["objectId"]),
                    ObjectType = (string)json["objectType"],

                    Elements = new Dictionary<string, object> { 
                            { "userId", Convert.ToInt32((string) json["userId"]) }, 
                            { "lastName", (string) json["lastName"] }, 
                            { "firstName", (string) json["firstName"] }
                        }
                };
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine(">>>" + ex.InnerException);
                node = null;
            }
            //});

            //thread.Start();
            //thread.Join(20 * 1000);

            return node;
        }

        public string GetServerName()
        {
            return serverName;
        }

        // Lots

        public void SetLots(List<Lot> lots, string operation = "O") {
            var content = SerializeListAsCsvPost<Lot>(lots, (x) => {
                var row = SerializeItemAsCsvPostLine(operation, "H", "", "",
                    x.GetMobileRecordId(GetOrgNumber()),
                    functionalGroupMap["Service"],
                    GetOrgName(),
                    GetOrgNumber(),
                    x.Id.ToString(),
                    x.LotNumber,
                    ToUsDateString(x.ScheduleDate),
                    x.DumpStatus,
                    x.VarietyId.ToString(),
                    x.VarietyName,
                    x.CommodityId.ToString(),
                    x.CommodityName,
                    ToUsDateString(x.StartedDumpingDateTime),
                    ToUsDateString(x.FinishedDumpingDateTime),
                    x.GetFacilityId(),
                    "no"
                );

                return row;
            });

            var result = UploadCsvContent("shipservice", "setLots", content);
        }

        // Growers

        public void SetGrowerServiceTicket(Ticket ticket, string operation = "O") {
            SetGrowerServiceTickets(new List<Ticket> { ticket }, operation);
        }

        public void SetGrowerServiceTickets(List<Ticket> tickets, string operation)
        {
            var content = SerializeListAsCsvPost<Ticket>(tickets, (x) => {
                var row = SerializeItemAsCsvPostLine(operation, "H", "", "", 
                    x.GetMobileRecordId(GetOrgNumber()),
                    functionalGroupMap["Service"],
                    GetOrgName(),
                    GetOrgNumber(),
                    x.Number,
                    ToUsDateString(x.Date),
                    x.ReferenceNumber,
                    x.GetDriverFirstName(),
                    x.GetDriverLastName(),
                    x.DriverRecordId.ToString(),
                    x.GetVehicleCodeOrName(),
                    x.GetGrowerName(), 
                    x.GrowerRecordId.ToString(),
                    x.GetRanchName(), 
                    x.RanchRecordId.ToString(),
                    x.GetRanchAddress(),
                    x.GetBuyerFirstName(),
                    x.GetBuyerLastName(),
                    x.BuyerRecordId.ToString(),
                    x.GetVarietyName(),
                    x.TypeOfPick,
                    x.CountTotalBins().ToString(),
                    Rms.ToProcessed(x.DriverProcessed), 
                    x.PickUp.ToString(), 
                    x.Empties.ToString(), 
                    x.ActualPickUp.ToString(),
                    x.ActualDrop.ToString(),
                    x.TotalEmpties.ToString(), 
                    x.Timeout.ToString(),
                    x.Comments,
                    x.Latitude.ToString(),
                    x.Longitude.ToString(),
                    ToUsDateString(x.PickDateTime),
                    x.PurchaseOrderRecordId,
                    Rms.ToBooleanStr(x.AmPickUp),
                    x.EnteredBy,
                    x.ShortHauledBy,
                    x.LongHauledBy,
                    Rms.ToBooleanStr(x.FreskaStorage),
                    Rms.ToBooleanStr(x.Piru),
                    Rms.ToBooleanStr(x.MissedPickUp),
                    x.RootCause,
                    x.MissedPickUpComment,
                    x.PickUpId.ToString(),
                    Rms.ToBooleanStr(x.EmptiesPickupAll),
                    x.EmptiesPickupQuantity.ToString(),
                    x.CarryOver.ToString(),
                    x.Status, 
                    "FALSE"
                );

                var lines = SerializeListAsCsvPost<TicketDetail>(x.TicketDetails, (y) => {
                    return SerializeItemAsCsvPostLine(operation, "D", "", "", 
                        y.GetMobileRecordId(GetOrgNumber(), x.Number),
                        functionalGroupMap["Service"],
                        GetOrgName(),
                        GetOrgNumber(),
                        x.Number,
                        y.GetBinNumber(),
                        y.GetBinLevelQuantity().ToString(),
                        ToUsDateString(x.Date),
                        y.GetBinNumber(),
                        //Manufacturer Serial Number 
                        y.Status,
                        y.Weight.ToString()
                    );
                });

                return row + (!string.IsNullOrWhiteSpace(lines) ? CsvRowNeedle + lines : "");
            });

            var result = UploadCsvContent("shipservice", "setGrowerServiceTickets", content);
        }

        public List<Node> GetRecordCodingValuesExist(string codingName, List<Ticket> tickets) {
            var json = UploadCsvContent("recordservice", "getRecordCodingValuesExist/CodingName" + codingName,
                SerializeListAsCsvPost<Ticket>(tickets, (x) => {
                    return SerializeItemAsCsvPostLine("O", "H", "", "",
                        x.Number
                    );
                }));

            //return (from x in json select Node.Parse(x as JObject)).ToList();
            return null;
        }

        // Stores

        private const string StoresRootName = "Stores";

        public Node GetStoresRoot() {
            return GetDirectoryId("Storage", StoresRootName);
        }

        public string GetDeploymentFromStoreType(string storeType) {
            if (storeType == null)
                return "";

            storeType = storeType.ToLower();

            switch (storeType) {
                case "factory":
                case "lab":
                    return "inventory";

                case "carrier":
                    return "";

                case "customer":
                    return "field";

                case "vendor":
                    return "field";

                default:
                    return "";
            }
        }

        public List<Store> GetStores(Node storesRoot, string maxTimestamp = null) {
            var result = new List<Store>();
            var rmsStores = GetChildrenDirectoryIds(storesRoot, "Store");

            foreach (var rmsStore in rmsStores) {
                var storeCodingInfo = GetRecordCoding(rmsStore);
                result.Add(Store.Parse(rmsStore, storeCodingInfo));
            }

            var rmsStoresLetters = GetChildrenDirectoryIds(storesRoot, "Storage");

            foreach (var rmsStoreLetter in rmsStoresLetters) {
                var rmsStoresInLetters = GetChildrenDirectoryIds(rmsStoreLetter, "Store");

                foreach (var rmsStore in rmsStoresInLetters) {
                    var storeCodingInfo = GetRecordCoding(rmsStore);
                    result.Add(Store.Parse(rmsStore, storeCodingInfo));
                }
            }

            return result;
        }

        public void CreateStore(Node storesRoot, string storeName, string storeType) {
            var parentTreeId = Convert.ToString(Convert.ToInt32(storesRoot.Elements["treeId"]));
            var store = CreateStore(parentTreeId, storeName);

            SetRecordCodings(new Rms.Node { ObjectId = store.ObjectId, ObjectType = store.ObjectType },
                new Dictionary<string, string> {
                    { "Organization Name", GetOrgName() }, 
                    { "Organization Number", GetOrgNumber() }, 
                    { "Store Name", storeName }, 
                    { "Sync Non Tracked Items", "true" }, 
                    { "ItemType", "store" }, 
                    { "Store Type", storeType }, 
                    { "Directory Style", "track" }, 
                    { "Alias Name", storeName }
                });
        }

        public Node SetStores(List<Store> stores) {
            var result = UploadCsvContent("partservice", "setStores",
                SerializeListAsCsvPost<Store>(stores, (x) => {
                    return SerializeItemAsCsvPostLine("O", "H", "", "",
                        "",                                                                                     // MobileRecordId
                        functionalGroupMap["Store"],                                                            // Functional Group Name
                        GetOrgName(),                                                                           // Org Name
                        GetOrgNumber(),                                                                         // Org Number
                        x.StoreName,                                                                            // Store Name
                        "",                                                                                     // Store Number
                        "",                                                                                     // Store Id
                        x.StoreType,                                                                            // Store Type
                        "",                                                                                     // Center Name
                        "",                                                                                     // Center Number
                        "",                                                                                     // Company
                        "",                                                                                     // First Name
                        "",                                                                                     // Last Name
                        "",                                                                                     // Address1
                        "",                                                                                     // City
                        "",                                                                                     // State
                        "",                                                                                     // ZipCode
                        "",                                                                                     // Country
                        "",                                                                                     // Phone
                        string.IsNullOrWhiteSpace(x.Deployment) ? GetDeploymentFromStoreType(x.StoreType) : x.Deployment,     // Deployment
                        "",                                                                                     // Location
                        "TRUE",                                                                                 // IsaLocation
                        "",                                                                                     // Store Factor
                        "",                                                                                     // Alert Functional Group
                        "trackmod1000",                                                                         // Directory Style
                        "",                                                                                     // Latitude
                        "",                                                                                     // Longitude
                        "",                                                                                     // OwnerRecordId
                        "",                                                                                     // Temperature1
                        ""                                                                                      // Store Class
                    );
                }));

            if (result == null)
                return null;

            var json = JsonConvert.DeserializeObject<JArray>(result);

            return new Node {
                ObjectId = Convert.ToInt32(((JValue)((JObject)json[0]).Property("LobjectId").Value).ToString()),
                ObjectType = ((JObject)json[0]).Property("objectType").Value.ToString().Replace(CsvQuote.ToString(), ""),
                RecordId = ((JObject)((JObject)json[0]).Property("mapCodingInfo").Value).Property("RecordId").Value.ToString().Replace(CsvQuote.ToString(), "")
            };

            //if (result == null)
            //    return null;

            //var resultList = JArray.Parse(result);

            //if (resultList == null || resultList.Count == 0)
            //    return null;

            //var jsonResult = resultList.ToList();

            //return (from x in jsonResult select new Node {
            //    ObjectId = (int)x["LobjectId"],
            //    ObjectType = (string)x["objectType"]
            //}).ToList();
        }

        public List<Node> SetStores(List<Tuple<string, string, string>> stores) {
            var result = UploadCsvContent("partservice", "setStores",
                SerializeListAsCsvPost<Tuple<string, string, string>>(stores, (x) => {
                    return SerializeItemAsCsvPostLine("O", "H", "", "",
                        "",                                                                                     // MobileRecordId
                        functionalGroupMap["Store"],                                                            // Functional Group Name
                        GetOrgName(),                                                                           // Org Name
                        GetOrgNumber(),                                                                         // Org Number
                        x.Item1,                                                                                // Store Name
                        "",                                                                                     // Store Number
                        "",                                                                                     // Store Id
                        x.Item2,                                                                                // Store Type
                        "",                                                                                     // Center Name
                        "",                                                                                     // Center Number
                        "",                                                                                     // Company
                        "",                                                                                     // First Name
                        "",                                                                                     // Last Name
                        "",                                                                                     // Address1
                        "",                                                                                     // City
                        "",                                                                                     // State
                        "",                                                                                     // ZipCode
                        "",                                                                                     // Country
                        "",                                                                                     // Phone
                        "",                                                                                     // Fax
                        string.IsNullOrWhiteSpace(x.Item3) ? GetDeploymentFromStoreType(x.Item2) : x.Item3,     // Deployment
                        "",                                                                                     // Location
                        "",                                                                                     // Alias Location
                        "",                                                                                     // Alias Name
                        "",                                                                                     // Alias Scan
                        "TRUE",                                                                                 // IsaLocation
                        "",                                                                                     // Store Factor
                        "",                                                                                     // Alert Functional Group
                        "trackmod1000"                                                                          // Directory Style
                    );
                }));

            if (result == null)
                return null;

            var resultList = JArray.Parse(result);

            if (resultList == null || resultList.Count == 0)
                return null;

            var jsonResult = resultList.ToList();

            return (from x in jsonResult select new Node {
                ObjectId = (int) x["LobjectId"],
                ObjectType = (string) x["objectType"]
            }).ToList();
        }

        // Purchase orders

        public List<PurchaseOrder> GetPurchaseOrders(string maxTimestamp = null) {
            var json = InvokeJson<JArray>("quickbookservice", "getPurchaseOrders", "");

            if (json == null || json.Count == 0)
                return null;

            var result = new List<PurchaseOrder>();
            var list1 = json.ToList();

            foreach (JObject item in list1)
                result.Add(PurchaseOrder.Parse(item));

            return result;
        }

        public Node SetPurchaseOrder(PurchaseOrder po) {
            var content = SerializeListAsCsvPost<PurchaseOrder>(new List<PurchaseOrder> { po }, (x) => {
                var row = SerializeItemAsCsvPostLine("O", "H", "", "",
                    x.MobileRecordId,
                    functionalGroupMap["Purchase Order Header"],
                    GetOrgName(),
                    GetOrgNumber(),
                    x.PurchaseOrderNumber,
                    ToUsDateString(x.Date),
                    x.VendorName,
                    x.VendorRecordId,
                    x.DropShipTo,
                    x.Processed,
                    x.ProcessedDate,
                    x.ReceiveStoreName,
                    x.ReceiveStoreRecordId,
                    x.ReceivedFromStoreName,
                    x.ReceivedFromStoreRecordId,
                    x.DestinationStoreName,
                    x.DestinationStoreRecordId,
                    x.Tax,
                    x.TaxCode,
                    x.Freight,
                    x.Notes,
                    x.MobilePurchaseOrder,
                    x.DeviceId,
                    x.ItemType
                );

                var lines = SerializeListAsCsvPost<PurchaseOrderDetail>(x.Details, (y) => {
                    return SerializeItemAsCsvPostLine("O", "D", "", "",
                        y.MobileRecordId,
                        functionalGroupMap["Purchase Order Detail"],
                        GetOrgName(),
                        GetOrgNumber(),
                        x.PurchaseOrderNumber,
                        "", // mobilePurchaseOrder
                        y.ItemNumber,
                        y.Description,
                        y.UnitOfMeasure,
                        y.Quantity,
                        y.QuantityReceived,
                        y.QuantityLastReceived,
                        y.Cost,
                        y.Amount,
                        y.Billable,
                        ToUsDateString(y.Date),
                        y.CustomerJob,
                        y.LocationRecordId,
                        y.Processed,
                        y.ProcessedDate,
                        y.Notes,
                        y.DeviceId,
                        y.ItemType
                    );
                });

                return row + (!string.IsNullOrWhiteSpace(lines) ? CsvRowNeedle + lines : "");
            });

            var result = UploadCsvContent("quickbookservice", "setPurchaseOrders", content);

            if (result == null)
                return null;

            var json = JsonConvert.DeserializeObject<JArray>(result);

            return new Node {
                ObjectId = Convert.ToInt32(((JValue)((JObject)json[0]).Property("LobjectId").Value).ToString()),
                ObjectType = ((JObject)json[0]).Property("objectType").Value.ToString().Replace(CsvQuote.ToString(), ""),
                RecordId = ((JObject)((JObject)json[0]).Property("mapCodingInfo").Value).Property("RecordId").Value.ToString().Replace(CsvQuote.ToString(), "")
            };
        }

        public void SetPurchaseOrders(List<PurchaseOrder> purchaseOrders) {
            var content = SerializeListAsCsvPost<PurchaseOrder>(purchaseOrders, (x) => {
                var row = SerializeItemAsCsvPostLine("O", "H", "", "",
                    x.MobileRecordId,
                    functionalGroupMap["Purchase Order Header"],
                    GetOrgName(),
                    GetOrgNumber(),
                    x.PurchaseOrderNumber,
                    ToUsDateString(x.Date),
                    x.VendorName,
                    x.VendorRecordId,
                    x.DropShipTo,
                    x.Processed,
                    x.ProcessedDate,
                    x.ReceiveStoreName,
                    x.ReceiveStoreRecordId,
                    x.ReceivedFromStoreName,
                    x.ReceivedFromStoreRecordId,
                    x.DestinationStoreName,
                    x.DestinationStoreRecordId,
                    x.Tax,
                    x.TaxCode,
                    x.Freight,
                    x.Notes,
                    x.MobilePurchaseOrder,
                    x.DeviceId,
                    x.ItemType
                );

                var lines = SerializeListAsCsvPost<PurchaseOrderDetail>(x.Details, (y) => {
                    return SerializeItemAsCsvPostLine("O", "D", "", "",
                        "", // MobileRecordId
                        functionalGroupMap["Purchase Order Detail"],
                        GetOrgName(),
                        GetOrgNumber(),
                        x.PurchaseOrderNumber,
                        "", // mobilePurchaseOrder
                        y.ItemNumber,
                        y.Description,
                        y.UnitOfMeasure,
                        y.Quantity,
                        y.QuantityReceived,
                        y.QuantityLastReceived,
                        y.Cost,
                        y.Amount,
                        y.Billable,
                        ToUsDateString(y.Date),
                        y.CustomerJob,
                        y.LocationRecordId,
                        y.Processed,
                        y.ProcessedDate,
                        y.Notes,
                        y.DeviceId,
                        y.ItemType
                    );
                });

                return row + (!string.IsNullOrWhiteSpace(lines) ? CsvRowNeedle + lines : "");
            });

            var result = UploadCsvContent("quickbookservice", "setPurchaseOrders", content);
        }

        public void SetPurchaseOrdersOld(List<PurchaseOrder> purchaseOrders) {
            var content = SerializeListAsCsvPost<PurchaseOrder>(purchaseOrders, (x) => {
                var row = SerializeItemAsCsvPostLine("O", "H", "", "",
                    GetOrgName(),
                    GetOrgNumber(),
                    functionalGroupMap["Purchase Order Header"],
                    x.PurchaseOrderNumber,
                    ToUsDateString(x.Date),
                    x.VendorName,
                    x.VendorRecordId,
                    x.DropShipTo,
                    x.Processed,
                    x.ProcessedDate,
                    x.ReceiveStoreName,
                    x.ReceiveStoreRecordId,
                    x.ReceivedFromStoreName,
                    x.ReceivedFromStoreRecordId,
                    x.DestinationStoreName,
                    x.DestinationStoreRecordId,
                    x.Tax,
                    x.TaxCode,
                    x.Freight,
                    x.Notes,
                    x.MobileRecordId,
                    x.MobilePurchaseOrder,
                    x.DeviceId,
                    x.ItemType
                );

                var lines = SerializeListAsCsvPost<PurchaseOrderDetail>(x.Details, (y) => {
                    return SerializeItemAsCsvPostLine("O", "D", "", "",
                        "", // MobileRecordId
                        functionalGroupMap["Purchase Order Detail"],
                        GetOrgName(),
                        GetOrgNumber(),
                        x.PurchaseOrderNumber,
                        "",
                        y.ItemNumber,
                        y.Description,
                        y.UnitOfMeasure,
                        y.Quantity,
                        y.QuantityReceived,
                        y.QuantityLastReceived,
                        y.Cost,
                        y.Amount,
                        y.Billable,
                        ToUsDateString(y.Date),
                        y.CustomerJob,
                        y.LocationRecordId,
                        y.Processed,
                        y.ProcessedDate,
                        y.Notes,
                        y.DeviceId,
                        y.ItemType
                    );
                });

                return row + (!string.IsNullOrWhiteSpace(lines) ? CsvRowNeedle + lines : "");
            });

            var result = UploadCsvContent("quickbookservice", "setPurchaseOrders", content);
        }

        // Product types

        public Node SetProductTypes(List<ProductType> productTypes) {
            var result = UploadCsvContent("dataservice", "setProductTypes",
                SerializeListAsCsvPost<ProductType>(productTypes, (x) => {
                    return SerializeItemAsCsvPostLine("O", "H", "", "", "", // mobileRecordId
                        functionalGroupMap["Service"], 
                        GetOrgName(),
                        GetOrgNumber(),
                        x.Name,
                        x.Number.ToString(),
                        x.Category,
                        x.ItemType
                    );
                }));

            if (result == null)
                return null;

            var json = JsonConvert.DeserializeObject<JArray>(result);

            return new Node {
                ObjectId = Convert.ToInt32(((JValue)((JObject)json[0]).Property("LobjectId").Value).ToString()),
                ObjectType = ((JObject)json[0]).Property("objectType").Value.ToString().Replace(CsvQuote.ToString(), ""),
                RecordId = ((JObject)((JObject)json[0]).Property("mapCodingInfo").Value).Property("RecordId").Value.ToString().Replace(CsvQuote.ToString(), "")
            };
        }

        // Users

        public List<User> GetUsers(string maxTimestamp = null) {
            var json = InvokeJson<JArray>("userservice", "getUsers", maxTimestamp ?? "0");
            var result = new List<User>();

            if (json != null && json.Count > 0)
                foreach (JObject item in json)
                    result.Add(User.Parse(item));

            return result;
        }

        public Node SetUsers(List<User> users) {
            var result = UploadCsvContent("userservice", "setUsers",
                SerializeListAsCsvPost<User>(users, (x) => {
                    return SerializeItemAsCsvPostLine("O", "H", "",  "", "", // mobileRecordId
                        functionalGroupMap["User"],
                        x.UserGroup,
                        "NormalUser",
                        x.Username,
                        x.Password ?? "",
                        GetOrgName(),
                        GetOrgNumber(),
                        x.CompanyName,
                        x.LastName,
                        x.FirstName,
                        x.Address1,
                        x.Address2,
                        x.City,
                        x.State,
                        x.ZipCode,
                        "", // Country
                        x.Email,
                        x.Telephone,
                        x.ItemType,
                        "", // Lat
                        "", // Lon
                        "", // Driver License Number
                        "", // Driver License State
                        "", // Location
                        "", // Customer Number
                        "", // Date of Hire
                        "", // Date of Birth
                        x.UserType ?? "",
                        "" // Employee Id
                    );
                }));

            if (result == null)
                return null;

            var json = JsonConvert.DeserializeObject<JArray>(result);

            return new Node {
                ObjectId = Convert.ToInt32(((JValue)((JObject)json[0]).Property("LobjectId").Value).ToString()),
                ObjectType = ((JObject)json[0]).Property("objectType").Value.ToString().Replace(CsvQuote.ToString(), ""),
                RecordId = ((JObject)((JObject)json[0]).Property("mapCodingInfo").Value).Property("RecordId").Value.ToString().Replace(CsvQuote.ToString(), "")
            };
        }

        public List<FunctionalGroupMap> GetFunctionalGroupMap() {
            var json = InvokeJson<JArray>("userservice", "getFunctionalGroupMaps", "");
            var result = new List<FunctionalGroupMap>();

            if (json != null && json.Count > 0)
                foreach (var e in json)
                    result.Add(new FunctionalGroupMap {
                        FunctionalGroupObjectId = Convert.ToInt32((string) e["functionalGroupObjectId"]),
                        FunctionalGroupName = (string) e["functionalGroupName"],
                        RecordType = (string) e["recordType"]
                    });

            return result;
        }

        public void LoadFunctionalGroupMap() {
            functionalGroupMap = new Dictionary<string, string>();

            var functionalGroupMapsRet = GetFunctionalGroupMap();

            foreach (var map in functionalGroupMapsRet) {
                if (functionalGroupMap.ContainsKey(map.RecordType))
                    throw new Exception("'" + map.RecordType + "' is a duplicated functional group map. Unable to continue, please clean RMS settings first.");

                functionalGroupMap.Add(map.RecordType, map.FunctionalGroupName);
            }
        }

        // Organization

        public bool ExistsOrg() {
            return OrgCodingFields != null;
        }

        public string GetOrgName() {
            return orgName;
        }

        public string GetOrgNumber() {
            return OrgCodingFields["Organization Number"];
        }

        public Dictionary<string, string> OrgCodingFields {
            get {
                if (orgCodingFields != null)
                    return orgCodingFields;

                try {
                    orgCodingFields = GetRecordCoding(GetDirectoryId("Organization", orgName));
                    return orgCodingFields;
                } catch (Exception ex) {
                    Console.WriteLine("Unable to resolve organization '" + orgName + "'.", true);
                    return null;
                }
            }
        }

        private Dictionary<string, string> orgCodingFields; // Lazy-loaded

        // Low-level rest ports

        public Node CreateStore(string parentBarcode, string location, string itemType, string storeId, string storeName, string storeNumber, string invoiceNumber, string vendorRmaNumber, string destinationStoreNumber) {
            var json = InvokeJson<JArray>("partservice", "createStore", parentBarcode + "/" + location + "/" + itemType + "/" + storeId + "/" + storeName + "/" + storeNumber + "/" + invoiceNumber + "/" + vendorRmaNumber + "/" + destinationStoreNumber);

            return new Node {
                ObjectId = (int) json[0]["objectId"],
                ObjectType = (string) json[0]["objectType"],

                Elements = null
            };
        }

        public Node CreateStore(string recordName, string parentObjectId, string parentObjectType) {
            return CreateRecord("Store", recordName, "store", DateTime.Now.ToString("yyyy-MM-dd"), parentObjectId, parentObjectType);
        }

        public Node CreateStore(string parentTreeId, string storeName) {
            return CreateRecordInDirectory(parentTreeId, "Store", storeName);
        }

        public Node CreateRecord(string recordType, string recordName, string itemType, string date, string parentObjectId, string parentObjectType) {
            var json = InvokeJson<JArray>("recordservice", "createRecord",
                recordType + "/" + recordName + "/" + itemType + "/" + date + "/" + parentObjectId + "/" + parentObjectType);

            return new Node {
                ObjectId = (int) json[0]["objectId"],
                ObjectType = (string) json[0]["objectType"],

                Elements = null
            };
        }

        public Node CreateRecordInDirectoryAndSetCodingFields(string parentTreeId, string displayType, string objectName, Dictionary<string, string> extras = null) {
            var directoryNode = CreateRecordInDirectory(parentTreeId, displayType, objectName);

            var codingFields = new Dictionary<string, string> {
                { "Organization Name", GetOrgName() }, 
                { "Organization Number", GetOrgNumber() }
            };

            if (extras != null && extras.Count > 0)
                foreach (var extra in extras)
                    codingFields.Add(extra.Key, extra.Value);

            SetRecordCodings(new Rms.Node { ObjectId = directoryNode.ObjectId, ObjectType = directoryNode.ObjectType }, codingFields);

            return directoryNode;
        }

        public Node CreateRecordInDirectory(string parentTreeId, string displayType, string objectName) {
            var json = InvokeJson<JArray>("directoryservice", "createRecordInDirectory",
                parentTreeId + "/" + displayType + "/" + objectName);

            return new Node {
                ObjectId = (int) json[0]["objectId"],
                ObjectType = (string) json[0]["objectType"],

                Elements = new Dictionary<string, object> { 
                    { "treeId", (int) json[0]["treeId"] }, 
                    { "name", (string) json[0]["name"] }, 
                    { "parentTreeId", (int) json[0]["parentTreeId"] } 
                }
            };
        }

        public List<Node> GetRecordsUpdatedXFiltered(string recordDisplayType, int maxNumberfullDataRecords, long maxTimestamp, DateTime? fromDate, DateTime? toDate, string ateRangeField, string filterFields, string strFieldDelim, string filterValues, string strValueDelim, bool? isFilterByFuncGroups, string includeFields)
        {
            var fromDateStr = fromDate == null ? "+" : fromDate.ToString();
            var toDateStr = toDate == null ? "+" : toDate.ToString();

            var filterFieldsStr = filterFields == null ? "+" : filterFields;
            var filterValuesStr = filterValues == null ? "+" : filterValues;
            var ateRangeFieldStr = ateRangeField == null ? "+" : ateRangeField;
            var strValueDelimStr = strValueDelim == null ? "+" : strValueDelim;
            var isFilterByFuncGroupsStr = isFilterByFuncGroups == null ? "+" : isFilterByFuncGroups.ToString();
            var includeFieldsStr = includeFields == null ? "+" : includeFields;

            var json = InvokeJson<JArray>("recordservice", "getRecordsUpdatedXFiltered", recordDisplayType + "/" + maxNumberfullDataRecords + "/" + maxTimestamp + "/" + fromDateStr + "/" + toDateStr + "/" + ateRangeFieldStr + "/" + filterFieldsStr + "/" + strFieldDelim + "/" + filterValuesStr + "/" + strValueDelimStr + "/" + isFilterByFuncGroupsStr + "/" + includeFieldsStr);

            var result = new List<Node>();

            if (json != null)
                foreach (var e in json) {
                    var node = new Node {
                        ObjectId = (int)e["LobjectId"],
                        ObjectType = (string)e["objectType"],
                        BarCode = (string)e["BarCode"],
                        ErrorCode = (string)e["errorCode"],
                        ErrorMessage = (string)e["errorMessage"],
                        CsvDataFilePath = (string)e["csvDataFilePath"],
                        ICsvRow = (int)e["iCsvRow"],
                        MobileRecordId = (string)e["mobileRecordId"],
                        MapCodingInfo = (JObject)e["mapCodingInfo"]
                    };

                    try { node.RecordId = (string)e["RecordId"]; } catch (Exception ex) { }

                    result.Add(node);
                }

            return result;
        }

        public List<JObject> GetRecordsUpdatedFiltered(string recordType, int fullDataLimit, int maxTimestamp, string filterCodingFieldName, string filterCodingFieldValue)
        {
            var json = InvokeJson<JArray>("recordservice", "getRecordsUpdatedFiltered", 
                recordType + "/" + fullDataLimit + "/" + maxTimestamp + "/" + filterCodingFieldName + "/" + filterCodingFieldValue);

            return (from x in json select x as JObject).ToList();
        }

        public List<Node> GetRecordIdsAll(string recordType) {
            var json = InvokeJson<JArray>("recordservice", "getRecordIdsAll", recordType);
            var result = new List<Node>();

            foreach (var e in json)
                result.Add(new Node {
                    ObjectId = Convert.ToInt32((string) e["objectId"]), ObjectType = (string) e["objectType"],

                    Elements = new Dictionary<string, object> { 
                        { "codingTimeStamp", (string) e["codingTimeStamp"] }, 
                        { "contentTimeStamp", (string) e["contentTimeStamp"] }
                    }
                });

            return result;
        }

        public List<Node> GetRecordIdsUpdated(string recordType, string timestamp) {
            var json = InvokeJson<JArray>("recordservice", "getRecordIdsUpdated", recordType + "/" + timestamp);

            return (from x in json select new Node {
                ObjectId = Convert.ToInt32((string) x["objectId"]), ObjectType = (string) x["objectType"],

                Elements = new Dictionary<string, object> { 
                    { "codingTimeStamp", (string) x["codingTimeStamp"] }, 
                    { "contentTimeStamp", (string) x["contentTimeStamp"] }
                }
            }).ToList();
        }

        public List<Node> GetRecordIdsUpdatedByRecordType(string recordType, string timestamp) {
            var json = InvokeJson<JArray>("recordservice", 
                "getRecordIdsUpdatedByRecordType", recordType + "/" + timestamp);

            return (from x in json select new Node {
                ObjectId = Convert.ToInt32((string) x["objectId"]), ObjectType = (string) x["objectType"],

                Elements = new Dictionary<string, object> { 
                    { "codingTimeStamp", (string) x["codingTimeStamp"] }, 
                    { "contentTimeStamp", (string) x["contentTimeStamp"] }
                }
            }).ToList();
        }

        public List<Node> GetRecordIdsAllByRecordType(string recordType) {
            var json = InvokeJson<JArray>("recordservice", 
                "getRecordIdsAllByRecordType", recordType);

            return (from x in json select new Node {
                ObjectId = Convert.ToInt32((string) x["objectId"]), ObjectType = (string) x["objectType"],

                Elements = new Dictionary<string, object> { 
                    { "codingTimeStamp", (string) x["codingTimeStamp"] }, 
                    { "contentTimeStamp", (string) x["contentTimeStamp"] }
                }
            }).ToList();
        }

        public Node GetDirectoryId(string displayType, string objectName) {
            var json = InvokeJson<JArray>("directoryservice", "getDirectoryId", displayType + "/" + objectName);

            return new Node {
                ObjectId = (int) json[0]["objectId"],
                ObjectType = (string) json[0]["objectType"],

                Elements = new Dictionary<string, object> { 
                    { "treeId", (int) json[0]["treeId"] }, 
                    { "name", (string) json[0]["name"] }, 
                    { "parentTreeId", (int) json[0]["parentTreeId"] } 
                }
            };
        }

        public List<Node> GetChildrenDirectoryIds(Node parentObject, string childDisplayType) {
            return GetChildrenDirectoryIds(parentObject.ObjectId, parentObject.ObjectType, childDisplayType);
        }

        public List<Node> GetChildrenDirectoryIds(int parentobjectId, string parentobjectType, string childDisplayType) {
            var json = InvokeJson<JArray>("directoryservice", "getChildrenDirectoryIds",
                parentobjectId.ToString() + "/" + parentobjectType + "/" + childDisplayType);

            return (from x in json select new Node {
                ObjectId = (int) x["objectId"], ObjectType = (string) x["objectType"],

                Elements = new Dictionary<string, object> {
                    { "treeId", (int) x["treeId"] }, 
                    { "name", (string) x["name"] }, 
                    { "parentTreeId", (int) x["parentTreeId"] }, 
                }
            }).ToList();
        }

        public Dictionary<string, string> GetRecordCoding(Node node) {
            return GetRecordCoding(node.ObjectId.ToString(), node.ObjectType);
        }

        public Dictionary<string, string> GetRecordCoding(string objectId, string objectType) {
            var json = InvokeJson<JArray>("recordservice", "getRecordCoding", objectId + "/" + objectType);

            return (from x in json select new {
                DisplayName = (string) (x["displayName"] ?? ""),
                Value = (string) (x["value"] ?? "")
            }).ToDictionary(x => x.DisplayName, x => x.Value);
        }

        public void SetRecordName(Node node, string name) {
            InvokeGet("recordservice", "setRecordName", node.ObjectId.ToString() + "/" + node.ObjectType + "/" + name + "/");
        }

        public void SetRecordCodings(Node node, Dictionary<string, string> codingFields, bool ignoreErrorsAndEmpties) {
            foreach (var field in codingFields)
                SetRecordCoding(node, field.Key, field.Value, ignoreErrorsAndEmpties);
        }

        public void SetRecordCoding(Node node, string codingName, string codingValue, bool ignoreErrorsAndEmpties) {
            SetRecordCoding(node.ObjectId, node.ObjectType, codingName, codingValue, ignoreErrorsAndEmpties);
        }

        public void SetRecordCodings(Node node, Dictionary<string, string> codingFields) {
            foreach (var field in codingFields)
                SetRecordCoding(node, field.Key, field.Value);
        }

        public void SetRecordCoding(Node node, string codingName, string codingValue) {
            SetRecordCoding(node.ObjectId, node.ObjectType, codingName, codingValue);
        }

        public void SetRecordCoding(int objectId, string objectType, string codingName, string codingValue) {
            SetRecordCoding(objectId, objectType, codingName, codingValue, false);
        }

        public void SetRecordCoding(int objectId, string objectType, string codingName, string codingValue, bool ignoreErrorsAndEmpties) {
            if (string.IsNullOrWhiteSpace(codingValue))
                codingValue = " ";

            codingValue = ClearSpecialChars(codingValue);

            if (ignoreErrorsAndEmpties) {
                try {
                    if (string.IsNullOrWhiteSpace(codingValue))
                        return;

                    InvokeGet("recordservice", "setRecordCoding", objectId.ToString() + "/" + objectType + "/" + codingName + "/" + codingValue + "/");
                } catch (Exception ex) {
                    Console.WriteLine("Error in coding field (object: " + objectId + "," + objectType + "): " + codingName + ": " + codingValue);
                }
            } else {
                if (string.IsNullOrWhiteSpace(codingValue))
                    return;

                InvokeGet("recordservice", "setRecordCoding", objectId.ToString() + "/" + objectType + "/" + codingName + "/" + codingValue + "/");
            }
        }

        public void DeleteRecordIgnoreErrors(Node n) {
            DeleteRecordIgnoreErrors(n.ObjectId.ToString(), n.ObjectType);
        }

        public void DeleteRecordIgnoreErrors(string objectId, string objectType) {
            try {
                InvokeGet("recordservice", "deleteRecord", objectId + "/" + objectType);
            } catch (Exception ex) {
                //Console.WriteLine("Unable to delete record " + objectId + ", " + objectType);
            }
        }

        public void DeleteRecord(Node node) {
            DeleteRecord(node.ObjectId.ToString(), node.ObjectType);
        }

        public void DeleteRecord(string objectId, string objectType) {
            InvokeGet("recordservice", "deleteRecord", objectId + "/" + objectType);
        }

        // Csv post

        private const string CsvFileName = "fields.txt";
        private const char CsvColNeedle = ',';
        private const char CsvRowNeedle = '\n';
        private const char CsvQuote = '"';

        public string UploadCsvContent(string service, string operation, string content) {
            var url = GetUrlInstance(service, operation);

            //Console.WriteLine(url, true);
            //Console.WriteLine("POST-CONTENT: " + content, true);

            return HttpClient.UploadFormFile(url, CsvFileName, "media", new MemoryStream(Encoding.ASCII.GetBytes(content)));
        }

        // Internal use

        private bool IsTimestampGreaterThen(string timestamp1, string timestamp2) {
            try {
                return StringOperations.IsNumeric(timestamp1) && StringOperations.IsNumeric(timestamp2) ?
                    Convert.ToInt64(timestamp1) > Convert.ToInt64(timestamp2) : false;
            } catch (Exception ex) {
                return false;
            }
        }

        public static long GetMaxTimestamp(params string[] timestamps) {
            long result = 0;

            if (timestamps == null || timestamps.Length == 0)
                return result;

            foreach (var timestamp in timestamps) {
                if (!StringOperations.IsNumeric(timestamp))
                    continue;

                var value = Convert.ToInt64(timestamp);

                if (value > result)
                    result = value;
            }

            return result;
        }

        public string GetUrlInstance(string service, string operation, string query) {
            return GetUrlInstance(service, operation) + query;
        }

        public string GetUrlInstance(string service, string operation) {
            return url + service + "/" + operation + "/" + credentialsString + "/";
        }

        public T InvokeJsonIgnoreErrors<T>(string service, string operation, string query) {
            try {
                if (query.Length > 0 && query.Substring(query.Length - 1, 1) != "/")
                    query += "/";

                query = query.Replace("///", "/ / /").Replace("//", "/ /");

                return JsonConvert.DeserializeObject<T>(InvokeGet(service, operation, query));
            } catch (Exception ex) {
                //Console.WriteLine("Exception in first JSON invoke", true);
                return default(T);
            }
        }

        public T InvokeJson<T>(string service, string operation, string query) {
            try {
                if (query.Length > 0 && query.Substring(query.Length - 1, 1) != "/")
                    query += "/";

                query = query.Replace("///", "/ / /").Replace("//", "/ /");
                return JsonConvert.DeserializeObject<T>(InvokeGet(service, operation, query));
            } catch (Exception ex) {
                //Console.WriteLine("Exception in first JSON invoke", true);

                try {
                    System.Threading.Thread.Sleep(2 * 1000);
                    return JsonConvert.DeserializeObject<T>(InvokeGet(service, operation, query));
                } catch (Exception ex2) {
                    throw ex2;
                }
            }
        }

        private string InvokeGet(string service, string operation, string query) {
            if (!string.IsNullOrWhiteSpace(query) && query[query.Length - 1] != '/')
                query += "/";

            query = query.Replace("â€“", " ");

            System.Diagnostics.Debug.WriteLine("Url: " + url + service + "/" + operation + "/" + credentialsString + "/" + query, true);
            var result = HttpClient.Get(url + service + "/" + operation + "/" + credentialsString + "/" + query);
            System.Diagnostics.Debug.WriteLine("Result: " + result);

            return result;
        }

        public static List<T> GetDetailsList<T>(JObject token, Func<JObject, T> Parser) {
            if (token["arDetailRecordData"] == null || ((JArray) token["arDetailRecordData"]).Count == 0)
                return null;

            var details = ((JArray) token["arDetailRecordData"]).ToList();
            var result = new List<T>();

            foreach (JObject detail in details) {
                var objectId = (int) detail["LobjectId"];
                var objectType = (string) detail["objectType"];
                var codingFields = Rms.GetCodingInfo(detail);

                result.Add(Parser(detail));
            }

            return result;
        }

        public static Dictionary<string, string> SerializeRecordCodingInfo(JObject token) {
            var arCodingInfo = (JArray) token["arCodingInfo"];
            var result = arCodingInfo.ToDictionary(x => (string) x["displayName"], y => (string) y["value"]);

            if (result == null)
                result = new Dictionary<string, string>();

            result.Add("objectType", (String) token["objectType"]);

            try {
                var lobjectId = (int) token["LobjectId"];
                result.Add("LobjectId", lobjectId.ToString());
                result.Add("objectId", lobjectId.ToString());
            } catch (Exception ex) {
                var objectId = (String) token["objectId"];
                result.Add("LobjectId", objectId);
                result.Add("objectId", objectId);
            }

            return result;
        }

        public static Dictionary<string, string> GetCodingInfo(JObject token) {
            var codingFieldsJsonArray = (JArray) token["arCodingInfo"];

            if (codingFieldsJsonArray == null)
                return null;

            return codingFieldsJsonArray.ToDictionary(x => (string) x["displayName"], y => (string) y["value"]);
        }

        public static string SerializeListAsCsvPost<T>(List<T> list, Func<T, string> Parser) {
            var result = new StringBuilder();

            if (list != null)
                foreach (var item in list)
                    result.Append(Parser(item)).Append(CsvRowNeedle);

            return result.ToString().TrimEnd(CsvRowNeedle);
        }

        public static string SerializeItemAsCsvPostLine(params string[] values) {
            var result = new StringBuilder();

            foreach (var v in values)
                result.Append(CsvQuote).Append(ParseSpecialChars(v) ?? "").Append(CsvQuote).Append(CsvColNeedle);

            return result.ToString().TrimEnd(CsvColNeedle);
        }

        public static string ClearSpecialChars(string value) {
            if (string.IsNullOrWhiteSpace(value))
                return "";

            return value.Replace("/", "").Replace("#", "").Replace("\"", "''").Replace("\r", " ").Replace("\n", " ");
        }

        public static string ParseSpecialChars(string content) {
            if (content == null)
                return null;

            // NOTE: Chars reference - http://www.table-ascii.com/

            return content
                .Replace("%", @"\x11") //.Replace("%", @"\x25")
                .Replace("\"", @"\x22")
                .Replace("\r\n", @"\x0A0D");
        }

        public static string ParseSpecialPartsChars(string content) {
            if (content == null)
                return null;

            return content
                .Replace("\"", @" inch")
                .Replace("\x22", " inch");
        }

        // Chunkanizer

        private const int chunkSize = 100;

        public void Chunkanizer<T>(Action<List<T>> SetCall, List<T> list) {
            if (list == null || list.Count == 0)
                return;

            int chunksTotal = (list.Count / chunkSize) + (list.Count % chunkSize > 0 ? 1 : 0);

            for (int i=0; i<chunksTotal; i++) {
                var chunkList = GetListChunk<T>(list, i, chunkSize);
                SetCall(chunkList);
            }
        }

        public List<T> GetListChunk<T>(List<T> list, int chunkIndex, int chunkSize) {
            if (list == null)
                return null;

            if (chunkSize >= list.Count)
                return list;

            var chunkStartIndex = chunkIndex * chunkSize;
            var chunkEndIndex = chunkStartIndex + chunkSize;
            var result = new List<T>();

            for (int i=chunkStartIndex; i < list.Count && i < chunkEndIndex; i++)
                result.Add(list[i]);

            return result;
        }

        // Service specific cast helpers

        public static string ToProcessed(bool value) {
            return value ? "yes" : "no";
        }

        public static bool ToBoolean(string value) {
            if (string.IsNullOrEmpty(value))
                return false;

            return value == "1" || value.ToLower() == "true" || value.ToLower() == "yes";
        }

        public static bool? ToBooleanOrNull(string value) {
            if (string.IsNullOrEmpty(value))
                return null;

            return value == "1" || value.ToLower() == "true" || value.ToLower() == "yes";
        }

        public static string ToBooleanStr(bool? value) {
            return value == null || value == false ? "FALSE" : "TRUE";
        }

        public static decimal ToDecimal(string value) {
            if (string.IsNullOrEmpty(value))
                return 0;

            return Convert.ToDecimal(value);
        }

        public static double? ToNullableDouble(string value) {
            if (string.IsNullOrEmpty(value))
                return null;

            return Convert.ToDouble(value);
        }

        public static int? ToNullableInt32(string value) {
            if (string.IsNullOrEmpty(value))
                return null;

            return Convert.ToInt32(value);
        }

        public static double ToDouble(string value) {
            if (string.IsNullOrEmpty(value))
                return 0;

            return Convert.ToDouble(value);
        }

        public static int ToInt32(string value) {
            if (string.IsNullOrEmpty(value))
                return 0;

            return Convert.ToInt32(value);
        }

        public static DateTime? FromMilitaryDateTime(string yyyymmddHHmmss) {
            try {
                return Convert.ToDateTime(yyyymmddHHmmss);
            } catch (Exception ex) {
                return null;
            }
        }

        public static DateTime? ToDateTime(string mmddyyyy) {
            if (string.IsNullOrEmpty(mmddyyyy) || mmddyyyy.Trim().Length < 10)
                return null;

            var month = Convert.ToInt32(mmddyyyy.Substring(0, 2));
            var day = Convert.ToInt32(mmddyyyy.Substring(3, 2));
            var year = Convert.ToInt32(mmddyyyy.Substring(6, 4));

            return new DateTime(year, month, day);
        }

        public static Guid? ToNullableGuid(string guidStr) {
            try {
                return Guid.Parse(guidStr);
            } catch (Exception ex) {
                return null;
            }
        }

        public static long ToTimestampLong(string value) {
            return !string.IsNullOrWhiteSpace(value) ? Convert.ToInt64(value) : 0;
        }

        public static DateTime ToUnixDateTime(string unixTimestamp) {
            return new DateTime(1970, 1, 1, 0, 0, 0).AddSeconds(Convert.ToUInt64(unixTimestamp) / 1000).ToLocalTime();
        }

        public static string ToUnixTimestamp(DateTime value) {
            var span = (value - new DateTime(1970, 1, 1, 0, 0, 0, 0).ToLocalTime());
            return Convert.ToString(Convert.ToUInt64((double) span.TotalSeconds) * 1000);
        }

        public static string ToUsDateString(DateTime? value) {
            if (value == null)
                return null;

            return value.Value.ToString("MM/dd/yyyy");
        }

        public static string ToYyyyMmDdHhMmSsDateString(DateTime? value) {
            if (value == null)
                return null;

            return value.Value.ToString("yyyy-MM-dd HH:mm:ss");
        }

        public static string ToString(bool value) {
            return value ? "TRUE" : "FALSE";
        }

        public static string ToString(int? value) {
            if (value == null)
                return null;

            return value.Value.ToString();
        }

        public static string ToString(DateTime? value) {
            if (value == null)
                return null;

            return value.Value.ToString("yyyy-MM-dd HH:mm:ss");
        }

        // Service objects

        [DataContract]
        public sealed class ProductType : Rms.Node {
            [DataMember]
            public string CreationDate {
                get;
                set;
            }

            [DataMember]
            public string RecordId {
                get;
                set;
            }

            [DataMember]
            public string RmsTimestamp {
                get;
                set;
            }

            [DataMember]
            public string RmsCodingTimestamp {
                get;
                set;
            }

            public DateTime? RmsCodingTimestampInstance {
                get {
                    return Cast.ToDateTime(RmsCodingTimestamp);
                }
            }

            [DataMember]
            public string FunctionalGroupName {
                get;
                set;
            }

            [DataMember]
            public int? FunctionalGroupObjectId {
                get;
                set;
            }

            [DataMember]
            public string OrganizationName {
                get;
                set;
            }

            [DataMember]
            public string OrganizationNumber {
                get;
                set;
            }

            [DataMember]
            public string Name {
                get;
                set;
            }

            [DataMember]
            public int? Number {
                get;
                set;
            }

            [DataMember]
            public string Category {
                get;
                set;
            }

            [DataMember]
            public string ItemType {
                get;
                set;
            }
        }

        [DataContract]
        public sealed class Store: Rms.Node {
            [DataMember]
            public DateTime? CreationDate {
                get;
                set;
            }

            [DataMember]
            public string StoreFactor {
                get;
                set;
            }

            [DataMember]
            public string RecordId {
                get;
                set;
            }

            [DataMember]
            public string StoreName {
                get;
                set;
            }

            [DataMember]
            public string StoreNumber {
                get;
                set;
            }

            [DataMember]
            public string StoreType {
                get;
                set;
            }

            [DataMember]
            public string StoreId {
                get;
                set;
            }

            [DataMember]
            public string OrganizationName {
                get;
                set;
            }

            [DataMember]
            public string OrganizationNumber {
                get;
                set;
            }

            [DataMember]
            public string CenterName {
                get;
                set;
            }

            [DataMember]
            public string CenterNumber {
                get;
                set;
            }

            [DataMember]
            public string ItemType {
                get;
                set;
            }

            [DataMember]
            public string AisleStartNumber {
                get;
                set;
            }

            [DataMember]
            public string AisleEndNumber {
                get;
                set;
            }

            [DataMember]
            public string BayStartNumber {
                get;
                set;
            }

            [DataMember]
            public string BayEndNumber {
                get;
                set;
            }

            [DataMember]
            public string ShelfStartNumber {
                get;
                set;
            }

            [DataMember]
            public string ShelfEndNumber {
                get;
                set;
            }

            [DataMember]
            public string RMSCodingTimestamp {
                get;
                set;
            }

            [DataMember]
            public string RMSEfileTimestamp {
                get;
                set;
            }

            [DataMember]
            public string FirstName {
                get;
                set;
            }

            [DataMember]
            public string LastName {
                get;
                set;
            }

            [DataMember]
            public string Address1 {
                get;
                set;
            }

            [DataMember]
            public string City {
                get;
                set;
            }

            [DataMember]
            public string State {
                get;
                set;
            }

            [DataMember]
            public string ZipCode {
                get;
                set;
            }

            [DataMember]
            public string Country {
                get;
                set;
            }

            [DataMember]
            public string Phone {
                get;
                set;
            }

            [DataMember]
            public string Fax {
                get;
                set;
            }

            [DataMember]
            public string Company {
                get;
                set;
            }

            [DataMember]
            public string Deployment {
                get;
                set;
            }

            [DataMember]
            public string SyncNonTrackedItems {
                get;
                set;
            }

            [DataMember]
            public string DestinationStoreNumber {
                get;
                set;
            }

            [DataMember]
            public string DestinationStoreName {
                get;
                set;
            }

            [DataMember]
            public string InvoiceNumber {
                get;
                set;
            }

            [DataMember]
            public string PurchaseOrder {
                get;
                set;
            }

            [DataMember]
            public string ReceiveDate {
                get;
                set;
            }

            [DataMember]
            public string ReceiveTime {
                get;
                set;
            }

            [DataMember]
            public string ReceivedBy {
                get;
                set;
            }

            [DataMember]
            public string CustomerRmaNumber {
                get;
                set;
            }

            [DataMember]
            public string VendorRmaNumber {
                get;
                set;
            }

            [DataMember]
            public string Aisle {
                get;
                set;
            }

            [DataMember]
            public string Bay {
                get;
                set;
            }

            [DataMember]
            public string Shelf {
                get;
                set;
            }

            [DataMember]
            public string Location {
                get;
                set;
            }

            [DataMember]
            public string TrackingNumber {
                get;
                set;
            }

            [DataMember]
            public string TrackingStatus {
                get;
                set;
            }

            [DataMember]
            public string AliasLocation {
                get;
                set;
            }

            [DataMember]
            public string AliasName {
                get;
                set;
            }

            [DataMember]
            public string AliasScan {
                get;
                set;
            }

            [DataMember]
            public string LocationRecordId {
                get;
                set;
            }

            [DataMember]
            public string DirectoryStyle {
                get;
                set;
            }

            [DataMember]
            public string FunctionalGroupName {
                get;
                set;
            }

            [DataMember]
            public string FunctionalGroupObjectId {
                get;
                set;
            }

            [DataMember]
            public string IsaLocation {
                get;
                set;
            }

            [DataMember]
            public string IsInventory {
                get;
                set;
            }

            [DataMember]
            public string AlertFunctionalGroup {
                get;
                set;
            }

            [DataMember]
            public string VIN {
                get;
                set;
            }

            [DataMember]
            public string PIMId {
                get;
                set;
            }

            [DataMember]
            public string MedallionId {
                get;
                set;
            }

            public static Store Parse(Node node, Dictionary<string, string> fields) {
                return new Store {
                    ObjectId = node.ObjectId,
                    ObjectType = node.ObjectType,

                    CreationDate = Rms.ToDateTime(fields["Creation Date"]),
                    StoreFactor = fields["Store Factor"],
                    RecordId = fields["RecordId"],
                    StoreName = fields["Store Name"],
                    StoreNumber = fields["Store Number"],
                    StoreType = fields["Store Type"],
                    StoreId = fields["Store Id"],
                    OrganizationName = fields["Organization Name"],
                    OrganizationNumber = fields["Organization Number"],
                    CenterName = fields["Center Name"], 
                    CenterNumber = fields["Center Number"], 
                    ItemType = fields["ItemType"], 
                    AisleStartNumber = fields["Aisle Start Number"], 
                    AisleEndNumber = fields["Aisle End Number"], 
                    BayStartNumber = fields["Bay Start Number"], 
                    BayEndNumber = fields["Bay End Number"], 
                    ShelfStartNumber = fields["Shelf Start Number"], 
                    ShelfEndNumber = fields["Shelf End Number"], 
                    RMSCodingTimestamp = fields["RMS Coding Timestamp"], 
                    RMSEfileTimestamp = fields["RMS Efile Timestamp"], 
                    FirstName = fields["First Name"], 
                    LastName = fields["Last Name"], 
                    Address1 = fields["Address1"], 
                    City = fields["City"], 
                    State = fields["State"], 
                    ZipCode = fields["ZipCode"], 
                    Country = fields["Country"], 
                    Phone = fields["Phone"], 
                    Fax = fields["Fax"], 
                    Company = fields["Company"], 
                    Deployment = fields["Deployment"], 
                    SyncNonTrackedItems = fields["Sync Non Tracked Items"], 
                    DestinationStoreNumber = fields["Destination Store Number"], 
                    DestinationStoreName = fields["Destination Store Name"], 
                    InvoiceNumber = fields["Invoice Number"], 
                    PurchaseOrder = fields["Purchase Order"], 
                    ReceiveDate = fields["Receive Date"], 
                    ReceiveTime = fields["Receive Time"], 
                    ReceivedBy = fields["Received By"], 
                    CustomerRmaNumber = fields["Customer RMA Number"], 
                    VendorRmaNumber = fields["Vendor RMA Number"], 
                    Aisle = fields["Aisle"], 
                    Bay = fields["Bay"], 
                    Shelf = fields["Shelf"], 
                    Location = fields["Location"], 
                    TrackingNumber = fields["Tracking Number"], 
                    TrackingStatus = fields["Tracking Status"], 
                    AliasLocation = fields["Alias Location"], 
                    AliasName = fields["Alias Name"], 
                    AliasScan = fields["Alias Scan"], 
                    LocationRecordId = fields["LocationRecordId"], 
                    DirectoryStyle = fields["Directory Style"], 
                    FunctionalGroupName = fields["FunctionalGroupName"], 
                    FunctionalGroupObjectId = fields["FunctionalGroupObjectId"], 
                    IsaLocation = fields["IsaLocation"], 
                    IsInventory = fields["IsInventory"], 
                    AlertFunctionalGroup = fields["Alert Functional Group"], 
                    VIN = fields["VIN"], 
                    PIMId = fields["PIM Id"],
                    MedallionId = fields["Medallion Id"]
                };
            }

            public override string ToString() {
                return "Store Name: " + StoreName + " - Store Number: " + StoreNumber;
            }
        }

        [DataContract]
        public sealed class FunctionalGroupMap: Rms.Node {
            [DataMember]
            public int FunctionalGroupObjectId {
                get;
                set;
            }

            [DataMember]
            public string FunctionalGroupName {
                get;
                set;
            }

            [DataMember]
            public string RecordType {
                get;
                set;
            }
        }

        [DataContract]
        public sealed class PurchaseOrder: Rms.Node {
            public PurchaseOrder() {
                Details = new List<PurchaseOrderDetail>();
            }

            [DataMember]
            public string BarCode {
                get;
                set;
            }
            [DataMember]
            public DateTime? CreationDate {
                get;
                set;
            }
            [DataMember]
            public string RecordId {
                get;
                set;
            }
            [DataMember]
            public string RmsTimestamp {
                get;
                set;
            }
            [DataMember]
            public string RmsCodingTimestamp {
                get;
                set;
            }
            [DataMember]
            public string RmsEfileTimestamp {
                get;
                set;
            }
            [DataMember]
            public string FunctionalGroupName {
                get;
                set;
            }
            [DataMember]
            public string FunctionalGroupObjectId {
                get;
                set;
            }
            [DataMember]
            public string MobileRecordId {
                get;
                set;
            }
            [DataMember]
            public string Year {
                get;
                set;
            }
            [DataMember]
            public string Month {
                get;
                set;
            }
            [DataMember]
            public string Day {
                get;
                set;
            }
            [DataMember]
            public string OrganizationName {
                get;
                set;
            }
            [DataMember]
            public string OrganizationNumber {
                get;
                set;
            }
            [DataMember]
            public string PurchaseOrderNumber {
                get;
                set;
            }
            [DataMember]
            public DateTime? Date {
                get;
                set;
            }
            [DataMember]
            public string VendorName {
                get;
                set;
            }
            [DataMember]
            public string VendorRecordId {
                get;
                set;
            }
            [DataMember]
            public string DropShipTo {
                get;
                set;
            }
            [DataMember]
            public string Processed {
                get;
                set;
            }
            [DataMember]
            public string ProcessedDate {
                get;
                set;
            }
            [DataMember]
            public string ReceiveStoreName {
                get;
                set;
            }
            [DataMember]
            public string ReceiveStoreRecordId {
                get;
                set;
            }
            [DataMember]
            public string ReceivedFromStoreName {
                get;
                set;
            }
            [DataMember]
            public string ReceivedFromStoreRecordId {
                get;
                set;
            }
            [DataMember]
            public string DestinationStoreRecordId {
                get;
                set;
            }
            [DataMember]
            public string DestinationStoreName {
                get;
                set;
            }
            [DataMember]
            public string Tax {
                get;
                set;
            }
            [DataMember]
            public string TaxCode {
                get;
                set;
            }
            [DataMember]
            public string Freight {
                get;
                set;
            }
            [DataMember]
            public string Notes {
                get;
                set;
            }
            [DataMember]
            public string MobilePurchaseOrder {
                get;
                set;
            }
            [DataMember]
            public string DeviceId {
                get;
                set;
            }
            [DataMember]
            public string ItemType {
                get;
                set;
            }

            public DateTime? RmsCodingTimestampInstance {
                get {
                    return Cast.ToDateTime(RmsCodingTimestamp);
                }
            }

            [DataMember]
            public List<PurchaseOrderDetail> Details {
                get;
                set;
            }

            [DataMember]
            public Ticket Ticket { get; set; }

            public Ticket GetPurchaseOrderBindedTicket(Node n) {
                this.BindPurchaseOrderToTicket(n);

                return Ticket;
            }

            public void BindPurchaseOrderToTicket(Node n) {
                BindPurchaseOrderToTicket(n.RecordId);
            }

            public void BindPurchaseOrderToTicket(string purchaseOrderRecordId) {
                if (Ticket == null)
                    return;

                this.Ticket.PurchaseOrderRecordId = purchaseOrderRecordId;
            }

            [DataMember]
            public object Related {
                get;
                set;
            }

            public void SetDayMonthYearFromDate() {
                var date = this.Date;

                if (date == null)
                    return;

                Day = date.Value.Day.ToString();
                Month = date.Value.Month.ToString();
                Year = date.Value.Year.ToString();
            }

            public PurchaseOrderDetail FindLineByItemNumber(string itemNumber) {
                if (Details == null || Details.Count == 0)
                    return null;

                foreach (var line in Details)
                    if (line.ItemNumber == itemNumber)
                        return line;

                return null;
            }

            public static PurchaseOrder Parse(JObject token) {
                var fields = Rms.GetCodingInfo(token);

                var purchaseOrder = new PurchaseOrder {
                    ObjectId = (int) token["LobjectId"],
                    ObjectType = (string) token["objectType"],
                    VendorName = fields["Vendor Name"],
                    Date = Rms.ToDateTime(fields["Date"]),
                    PurchaseOrderNumber = fields["Purchase Order"],
                    DropShipTo = fields["Drop Ship To"],
                    RecordId = fields["RecordId"],
                    Day = fields["Day"],
                    Month = fields["Month"],
                    Year = fields["Year"],
                    RmsCodingTimestamp = fields["RMS Coding Timestamp"],
                    MobilePurchaseOrder = fields["Mobile Purchase Order"],
                    VendorRecordId = fields["Vendor Record Id"]
                };

                purchaseOrder.Details = Rms.GetDetailsList<PurchaseOrderDetail>(token, PurchaseOrderDetail.Parse);
                return purchaseOrder;
            }

            public override string ToString() {
                return "RecordId:" + RecordId + ", VendorName:" + VendorName + ", VendorRecordId:" + VendorRecordId;
            }
        }

        [DataContract]
        public sealed class PurchaseOrderDetail: Rms.Node {
            public  PurchaseOrderDetail() {

            }

            public string BarCode {
                get;
                set;
            }
            public DateTime? CreationDate {
                get;
                set;
            }
            public string RecordId {
                get;
                set;
            }
            public string RmsTimestamp {
                get;
                set;
            }
            public string RmsCodingTimestamp {
                get;
                set;
            }
            public string RmsEfileTimestamp {
                get;
                set;
            }
            public string VendorName {
                get;
                set;
            }
            public string VendorRecordId {
                get;
                set;
            }
            public string MasterBarcode {
                get;
                set;
            }
            public string OrganizationName {
                get;
                set;
            }
            public string OrganizationNumber {
                get;
                set;
            }
            public string PurchaseOrder {
                get;
                set;
            }
            public string MobilePurchaseOrder {
                get;
                set;
            }
            public string ItemNumber {
                get;
                set;
            }
            public string Description {
                get;
                set;
            }
            public string UnitOfMeasure {
                get;
                set;
            }
            public string Quantity {
                get;
                set;
            }
            public string QuantityReceived {
                get;
                set;
            }
            public string QuantityLastReceived {
                get;
                set;
            }
            public string Cost {
                get;
                set;
            }
            public string Amount {
                get;
                set;
            }
            public string Billable {
                get;
                set;
            }
            public DateTime? Date {
                get;
                set;
            }
            public string CustomerJob {
                get;
                set;
            }
            public string LocationRecordId {
                get;
                set;
            }
            public string Processed {
                get;
                set;
            }
            public string ProcessedDate {
                get;
                set;
            }
            public string Notes {
                get;
                set;
            }
            public string DeviceId {
                get;
                set;
            }
            public string ItemType {
                get;
                set;
            }

            public DateTime? RmsCodingTimestampInstance {
                get {
                    return Cast.ToDateTime(RmsCodingTimestamp);
                }
            }

            public static List<PurchaseOrderDetail> ParseList(JArray token) {
                var json = token.ToString();

                var serializer = new System.Web.Script.Serialization.JavaScriptSerializer();
                return serializer.Deserialize<List<PurchaseOrderDetail>>(json);
            }

            public static PurchaseOrderDetail Parse(JObject token) {
                var fields = Rms.GetCodingInfo(token);

                var poDetail = new PurchaseOrderDetail {
                    ObjectId = (int) token["LobjectId"],
                    ObjectType = (string) token["objectType"],
                    //Quantity = Rms.ToDouble(fields["Quantity"]),
                    ItemNumber = fields["Item Number"],
                    Description = fields["Description"],
                    UnitOfMeasure = fields["U/M"],
                    RmsCodingTimestamp = fields["RMS Coding Timestamp"],
                    //QuantityReceived = Rms.ToDouble(fields["Quantity Received"]),
                    //Amount = Rms.ToDecimal(fields["Amount"]),
                    PurchaseOrder = fields["Purchase Order"],
                    RecordId = fields["RecordId"],
                    //Billable = Rms.ToBoolean(fields["Billable"]),
                    //Customer = fields["Customer:Job"],
                    ItemType = fields.ContainsKey("ItemType") ? fields["ItemType"] : fields["Item Type"],

                    //MobilePurchaseOrder = fields["Mobile Purchase Order"],
                    //Processed = fields["Processed"]
                };

                //if (fields.Keys.Contains("Quantity Last Recieved"))
                //    poDetail.QuantityLastReceived = Rms.ToDouble(fields["Quantity Last Recieved"]);

                return poDetail;
            }
        }

        [DataContract]
        public sealed class User : Rms.Node {
            [DataMember]
            public string Name {
                get;
                set;
            }

            [DataMember]
            public string Title {
                get;
                set;
            }

            [DataMember]
            public string FirstName {
                get;
                set;
            }

            [DataMember]
            public string LastName {
                get;
                set;
            }

            [DataMember]
            public string Username {
                get;
                set;
            }

            [DataMember]
            public string Password {
                get;
                set;
            }

            [DataMember]
            public string Address1 {
                get;
                set;
            }

            [DataMember]
            public string Address2 {
                get;
                set;
            }

            [DataMember]
            public string City {
                get;
                set;
            }

            [DataMember]
            public string State {
                get;
                set;
            }

            [DataMember]
            public string ZipCode {
                get;
                set;
            }

            [DataMember]
            public string CompanyName {
                get;
                set;
            }

            [DataMember]
            public string Telephone {
                get;
                set;
            }

            [DataMember]
            public string MobilePhone {
                get;
                set;
            }

            [DataMember]
            public string Email {
                get;
                set;
            }

            [DataMember]
            public string WorkerId {
                get;
                set;
            }

            [DataMember]
            public string RecordId {
                get;
                set;
            }

            [DataMember]
            public string UserGroup {
                get;
                set;
            }

            [DataMember]
            public string Department {
                get;
                set;
            }

            [DataMember]
            public string ItemType {
                get;
                set;
            }

            [DataMember]
            public string UserType {
                get;
                set;
            }

            [DataMember]
            public string RmsCodingTimestamp {
                get;
                set;
            }

            public DateTime? RmsCodingTimestampInstance {
                get {
                    return Cast.ToDateTime(RmsCodingTimestamp);
                }
            }

            public bool IsAdmin() {
                return
                    StringOperations.EqualsIgnoreCase(FirstName, "admin") ||
                    StringOperations.EqualsIgnoreCase(LastName, "admin") ||
                    StringOperations.EqualsIgnoreCase(Title, "admin") ||
                    StringOperations.EqualsIgnoreCase(Username, "admin");
            }

            public bool IsUsername(string username) {
                if (StringOperations.IsNullOrWhiteSpaceAny(Username, username))
                    return false;

                return StringOperations.EqualsIgnoreCase(Username, username);
            }

            public string Identity {
                get {
                    var suffix = !string.IsNullOrWhiteSpace(CompanyName) ? ", " + CompanyName : "";

                    if (!string.IsNullOrWhiteSpace(Username))
                        return Username + suffix;

                    return (FirstName ?? "") + " " + (LastName ?? "") + suffix;
                }
            }

            public override string ToString() {
                return
                    "First/Last Name: " + (FirstName ?? "") + " " + (LastName ?? "") +
                    ", Company: " + (CompanyName ?? "") +
                    ", Username: " + (Username ?? "") +
                    ", Password: " + (Password ?? "") +
                    ", WorkerId/AccountNo: " + (WorkerId ?? "") +
                    ", ItemType: " + (ItemType ?? "");
            }

            public static User Parse(Rms.Node node, Dictionary<string, string> codingFields) {
                return new User {
                    ObjectId = node.ObjectId,
                    ObjectType = node.ObjectType,

                    Title = codingFields["Title"],
                    FirstName = codingFields["First Name"],
                    LastName = codingFields["Last Name"],
                    Username = codingFields["Email"],
                    Password = null,
                    Address1 = codingFields["Address1"],
                    Address2 = codingFields["Address2"],
                    City = codingFields["City"],
                    State = codingFields["State"],
                    ZipCode = codingFields["ZipCode"],
                    CompanyName = codingFields["Company"],
                    Telephone = codingFields["Telephone"],
                    MobilePhone = codingFields["MobilePhone"],
                    Email = codingFields["Email"],
                    WorkerId = node.ObjectId.ToString(),
                    RecordId = codingFields["RecordId"],
                    ItemType = codingFields["ItemType"],
                    RmsCodingTimestamp = codingFields["RMS Coding Timestamp"]
                };
            }

            public static User Parse(JObject token) {
                var codingInfo = Rms.GetCodingInfo(token);

                return new User {
                    ObjectId = (int)token["LobjectId"],
                    ObjectType = (string)token["objectType"],

                    //CreationDate = codingInfo["Creation Date"],
                    Title = codingInfo["Title"],
                    FirstName = codingInfo["First Name"],
                    LastName = codingInfo["Last Name"],
                    Username = codingInfo["Email"],
                    Password = null,
                    CompanyName = codingInfo["Company"],
                    Address1 = codingInfo["Address1"],
                    Address2 = codingInfo["Address2"],
                    City = codingInfo["City"],
                    State = codingInfo["State"],
                    ZipCode = codingInfo["ZipCode"],
                    Email = codingInfo["Email"],
                    Telephone = codingInfo["Telephone"],
                    MobilePhone = codingInfo["MobilePhone"],
                    ItemType = codingInfo["ItemType"],
                    WorkerId = ((int)token["LobjectId"]).ToString(),
                    RecordId = codingInfo["RecordId"],
                    RmsCodingTimestamp = codingInfo["RMS Coding Timestamp"]
                };
            }
        }

        [DataContract]
        public class Node: INode {
            [DataMember]
            public int ObjectId {
                get;
                set;
            }

            [DataMember]
            public string ObjectType {
                get;
                set;
            }

            [DataMember]
            public string BarCode {
                get;
                set;
            }

            [DataMember]
            public string ErrorCode
            {
                get;
                set;
            }

            [DataMember]
            public string ErrorMessage
            {
                get;
                set;
            }

            [DataMember]
            public string CsvDataFilePath
            {
                get;
                set;
            }

            [DataMember]
            public int ICsvRow
            {
                get;
                set;
            }

            [DataMember]
            public string MobileRecordId {
                get;
                set;
            }

            [DataMember]
            public string RecordId {
                get;
                set;
            }

            [DataMember]
            public JObject MapCodingInfo
            {
                get;
                set;
            }

            [DataMember]
            public Dictionary<string, object> Elements
            {
                get;
                set;
            }

            public override string ToString() {
                return "objectId: " + ObjectId + ", objectType: " + ObjectType;
            }

            // Helpers

            public string GetCodingField(string key)
            {
                return (string) MapCodingInfo[key];
            }

            public long GetCodingFieldAsLong(string key)
            {
                return GetCodingFieldAsInt64(key);
            }

            public long GetCodingFieldAsInt64(string key)
            {
                return Convert.ToInt64(GetCodingField(key));
            }

            public int? GetCodingFieldAsNullableInt32(string key) {
                if (key == null || key.Trim() == "")
                    return null;

                return Convert.ToInt32(GetCodingField(key));
            }

            public decimal? GetCodingFieldAsNullableDecimal(string key) {
                if (key == null || key.Trim() == "")
                    return null;

                return Convert.ToDecimal(GetCodingField(key));
            }

            public int GetCodingFieldAsInt32(string key)
            {
                return Convert.ToInt32(GetCodingField(key));
            }

            public DateTime? GetCodingFieldAsNullableDateTime(string key) {
                return Rms.ToDateTime(GetCodingField(key));
            }
        }

        public interface INode {
            int ObjectId { get; set; }
            string ObjectType { get; set; }
        }
    }
}