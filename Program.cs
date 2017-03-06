using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WestPakMiddleware.Api;
using WestPakMiddleware.BusinessLogic.Access;
using WestPakMiddleware.BusinessLogic.SqlExpress;

namespace WestPakMiddleware {
    public class Program {
        private bool isListening = true;
        private static BusinessRules rules;

        public static void Main(string[] args) {
            Thread.CurrentThread.CurrentUICulture = new CultureInfo("en-US");
            Thread.CurrentThread.CurrentCulture = new CultureInfo("en-US");

            new Program(args);
        }

        public Program(string[] args) {
            // Initialize middleware

            try {
                Console.WriteLine("WestPak Middleware v0.0.1");
                rules = new BusinessRules();
            } catch (Exception ex) {
                Console.WriteLine("Unable to start." + Environment.NewLine + ex);
                return;
            }

            // Monitor WestPak SQL Express and RMS ticket related differences and keep them in sync

            new Thread(() => {
                rules.Initialize();

                Console.WriteLine("Service started (Syncing systems: " + rules.GetAccessDatabaseDsn() + " <-> " + rules.RmsName + " <-> " + BusinessRules.Organization + ")");
                Console.WriteLine("");

                if (rules.IsFirstSyncDateTime() || ExistsSetupParameter(args))
                    rules.SetupMissingRecordIds();

                ResetSyncTimestamps();
                
                while (isListening) {
                    try {
                        rules.StartCron();

                        SyncAccessPickUpSheetsToRms();
                        SyncRmsPopulatedTicketsToSql();
                        //SyncRmsPopulatedTicketsToAccess();
                        SyncEmptySqlLotsToRms();
                        SyncRmsPopulatedLotsToSql();
                    } catch (Exception ex) {
                        Console.WriteLine("Error: " + ex.ToString() + Environment.NewLine + ex.StackTrace);
                    } finally {
                        rules.StopCron();

                        Console.WriteLine("Done (spent " + rules.GetCronElapsedSecs() + "secs).");
                        Console.WriteLine("Waiting " + rules.SyncSecs + "secs for the next sync (Service can be stopped now)...");
                        Console.WriteLine("");

                        Thread.Sleep(rules.SyncSecs * BusinessRules.SecMillis);
                    }
                }
            }).Start();

            Console.ReadLine();
        }

        private void SyncAccessPickUpSheetsToRms() {
            try {
                var lastSyncTimestamp = rules.GetSyncDateTime();
                Console.WriteLine("Synchronizing Access -> RMS (Last sync timestamp: " + rules.FormatLastSyncDateStr(lastSyncTimestamp) + ")");

                var pickUpSheets = rules.GetAccessPickUpSheets(lastSyncTimestamp);

                if (pickUpSheets == null || pickUpSheets.Count == 0)
                    return;

                Console.WriteLine("  " + pickUpSheets.Count + " change(s)/new pickup sheet(s) found in Access");
                var purchaseOrders = new List<Rms.PurchaseOrder>();
                var counter = 1;

                foreach (var pickUpSheet in pickUpSheets) {
                    if (rules.ExistsTicketInRmsByPickUpId(pickUpSheet.Id.ToString()))
                        if (!rules.IsTicketEmptyByPickUpId(pickUpSheet.Id.ToString()))
                            continue;

                    var po = rules.TransformToRmsPurchaseOrderPlusTicket(pickUpSheet);
                    Console.WriteLine("   " + (counter++) + "/" + pickUpSheets.Count + ". Batching RMS PO #" + po.PurchaseOrderNumber);
                    purchaseOrders.Add(po);
                }

                if (purchaseOrders.Count > 0) {
                    Console.WriteLine("   Sending " + purchaseOrders.Count + " purchase order(s)");
                    rules.SetRmsPurchaseOrdersPlusTickets(purchaseOrders);

                    lastSyncTimestamp = rules.GetAccessNextSyncTimestamp(pickUpSheets);
                    rules.SetSyncDateTime(lastSyncTimestamp);
                }
            } catch (Exception ex) {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        private void SyncRmsPopulatedTicketsToSql() {
            try {
                var maxTimestamp = rules.GetRmsMaxTimestampAsLong();

                Console.WriteLine("Synchronizing RMS -> SQL (Max timestamp: " + maxTimestamp + ")");
                
                var rmsTickets = rules.GetRmsTicketsSinceTimestamp(maxTimestamp);

                if (rmsTickets == null || rmsTickets.Count == 0)
                    return;

                var total = rmsTickets.Count;
                var counter = 1;

                foreach (var rmsTicket in rmsTickets) {
                    Console.Write(" " + counter++ + "/" + total + " Syncing ticket " + rmsTicket.ToString() + " ");
                
                    var sqlTicket = rules.GetSqlTicket(rmsTicket.TicketNumber);
                    var existsSqlTicket = sqlTicket != null;

                    if (existsSqlTicket) {
                        if (rules.IsSqlTicketMoreRecent(sqlTicket, rmsTicket)) {
                            Console.WriteLine("(Skipping this ticket. Outdated in RMS)...");
                            continue;
                        }

                        Console.WriteLine("(Updating in SQL Express)...");
                        rules.UpdateSqlTicket(rmsTicket);
                    } else {
                        Console.WriteLine("(Creating in SQL Express)...");
                        rules.CreateSqlTicket(rmsTicket);
                    }
                }

                var newMaxTimestamp = rmsTickets.Max(x => x.RmsCodingTimestamp) + 1;
                rules.SetRmsMaxTimestampAsLong(newMaxTimestamp);
            } catch (Exception ex) {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        private void SyncRmsPopulatedTicketsToAccess() {
            try {
                var maxTimestamp = rules.GetRmsMaxTimestampAsLong("PopulatedTicketsToAccess");
                Console.WriteLine("Synchronizing populated tickets from SQL -> Access (Max timestamp: " + maxTimestamp + ")");

                var rmsTickets = rules.GetRmsTicketsSinceTimestamp(maxTimestamp);

                if (rmsTickets == null || rmsTickets.Count == 0)
                    return;

                var total = rmsTickets.Count;
                var counter = 1;

                foreach (var rmsTicket in rmsTickets) {
                    Console.Write(" " + counter++ + "/" + total + " Syncing pick-up sheets " + rmsTicket.ToString() + " ");

                    var sqlTicket = rules.GetSqlTicket(rmsTicket.TicketNumber);
                    var existsSqlTicket = sqlTicket != null;

                    //var accessPickUpSheet = rules.GetAccessPickUpSheet(rmsTicket.PickupId);

                    //if (existsSqlTicket && accessPickUpSheet) {
                    //    Console.WriteLine("(Updating PickUp Sheet)...");
                    //    rules.UpdatePickUpSheet(rmsTicket);
                    //}
                }

                //var newMaxTimestamp = rmsTickets.Max(x => x.RmsCodingTimestamp) + 1;
                //rules.SetRmsMaxTimestampAsLong(newMaxTimestamp);
            } catch (Exception ex) {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        private void SyncEmptySqlLotsToRms() {
            try {
                var lastSyncTimestamp = rules.GetSyncDateTime("EmptyLots");
                Console.WriteLine("Synchronizing empty lots from SQL -> RMS (Last sync timestamp: " + rules.FormatLastSyncDateStr(lastSyncTimestamp) + ")");

                var lots = rules.GetNewSqlEmptyLots(lastSyncTimestamp);

                if (lots == null || lots.Count == 0)
                    return;

                Console.WriteLine("  " + lots.Count + " change(s)/new lots found in Access");

                if (lots.Count > 0) {
                    var counter = 1;

                    foreach (var l in lots) {
                        Console.WriteLine("   " + (counter++) + "/" + lots.Count + ". Batching lot " + l.ToString());
                        l.SetVarietyAndCommodityNames(rules.FindSqlEntityName("Variety", l.VarietyId), rules.FindSqlEntityName("Commodity", l.CommodityId));
                    }

                    Console.WriteLine("   Sending " + lots.Count + " lot(s)");
                    rules.SetRmsEmptyLots(lots);

                    lastSyncTimestamp = rules.GetEmptyLotsNextSyncTimestamp(lots);
                    rules.SetSyncDateTime("EmptyLots", lastSyncTimestamp);
                }
            } catch (Exception ex) {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        private void SyncRmsPopulatedLotsToSql() {
            try {
                var maxTimestamp = rules.GetRmsMaxTimestampAsLong("PopulatedLots");
                Console.WriteLine("Synchronizing populated lots from RMS -> SQL (Max timestamp: " + maxTimestamp + ")");

                var rmsLots = rules.GetRmsLotsWithLotBinsSinceTimestamp(maxTimestamp);

                if (rmsLots == null || rmsLots.Count == 0)
                    return;

                var total = rmsLots.Count;
                var counter = 1;

                foreach (var rmsLot in rmsLots) {
                    Console.Write(" " + counter++ + "/" + total + " Syncing ticket " + rmsLot.ToString() + " ");

                    if (!rules.ExistsSqlLot(rmsLot)) {
                        Console.WriteLine("(Skipping this lot. Does not exists in SQL Express yet)...");
                        continue;
                    }

                    Console.WriteLine("(Updating lot and lot bins in SQL Express)...");
                    rules.UpdateSqlLotBins(rmsLot);
                }

                var newMaxTimestamp = rmsLots.Max(x => x.RmsCodingTimestamp) + 1;
                rules.SetRmsMaxTimestampAsLong("PopulatedLots", newMaxTimestamp);
            } catch (Exception ex) {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        #region Helpers

        private void ResetSyncTimestamps() {
            rules.SetSyncDateTime(DateTime.MinValue);
            rules.SetRmsMaxTimestampAsLong(0);
            rules.SetSyncDateTime("EmptyLots", DateTime.MinValue);
            rules.SetRmsMaxTimestampAsLong("PopulatedLots", 0);
        }

        private bool ExistsSetupParameter(string[] args) {
            if (args == null || args.Length == 0)
                return false;

            foreach (var arg in args)
                if (arg != null && arg.Replace("/", "").Equals("setup", StringComparison.CurrentCultureIgnoreCase))
                    return true;

            return false;
        }

        #endregion

        #region Temp

        private void SyncSqlToRms(DateTime? lastSyncTimestamp) {
            var sqlTickets = rules.GetSqlExpressTicketsSinceTimestamp(lastSyncTimestamp);

            if (sqlTickets == null || sqlTickets.Count == 0)
                return;

            Console.WriteLine("Synchronizing SQL -> RMS");
            var total = sqlTickets.Count;
            var counter = 1;

            foreach (var sqlTicket in sqlTickets) {
                Console.Write(" " + counter++ + "/" + total + " Syncing ticket " + sqlTicket.ToString() + " ");

                if (counter < 638) {
                    Console.WriteLine();
                    continue;
                }

                var rmsTicket = rules.GetRmsTicket(sqlTicket);
                var existsRmsTicket = rmsTicket != null;

                if (existsRmsTicket) {
                    if (rules.IsSqlTicketMoreRecent(sqlTicket, rmsTicket)) {
                        Console.WriteLine("(Updating)...");
                        rules.UpdateRmsTicket(sqlTicket, rmsTicket);
                    } else
                        Console.WriteLine("(Skipping, outdated)...");
                } else {
                    Console.WriteLine("(Creating)...");
                    rules.CreateTicketInRms(sqlTicket);
                }
            }
        }

        private DateTime SyncSqlToRmsOld(DateTime? lastSyncTimestamp) {
            var startSyncTime = DateTime.Now;

            var sqlTickets = rules.GetSqlExpressTicketsSinceTimestamp(lastSyncTimestamp);

            if (sqlTickets == null || sqlTickets.Count == 0)
                return startSyncTime;

            Console.WriteLine("Synchronizing SQL -> RMS");
            var total = sqlTickets.Count;
            var counter = 1;

            foreach (var sqlTicket in sqlTickets) {
                Console.Write(" " + counter++ + "/" + total + " Syncing ticket " + sqlTicket.ToString() + " ");

                //if (counter < 638) {
                //    Console.WriteLine();
                //    continue;
                //} 

                var rmsTicket = rules.GetRmsTicket(sqlTicket);
                var existsRmsTicket = rmsTicket != null;

                //if (existsRmsTicket) {
                //    if (rules.IsSqlTicketMoreRecent(sqlTicket, rmsTicket)) {
                //        Console.WriteLine("(Updating)...");
                //        rules.UpdateRmsTicket(sqlTicket, rmsTicket);
                //    } else
                //        Console.WriteLine("(Skipping, outdated)...");
                //} else {
                //    Console.WriteLine("(Creating)...");
                //    rules.CreateTicketInRms(sqlTicket);
                //}
            }

            return startSyncTime;
        }

        //if (rules.existsNewSqlTickets(sqlTickets, rmsTickets))
        //{
        //    // Add new SQL tickets in RMS
        //}

        //if (rules.existsUpdatedSqlTickets(sqlTickets, rmsTickets))
        //{
        //    // Get tickets with changes
        //    // Timestamps based approach (ticket header stamp + ticket detail stamp) usage is not possible
        //    // Identify specific business fields that could be updated after a ticket is created and use them to track updates
        //    // Update discovered tickets list in RMS
        //}

        //if (rules.existsRemovedSqlTicketsInRms(sqlTickets, rmsTickets))
        //{
        //    // Calculate tickets to remove (remove list = rmsTickets list - sqlTickets list)
        //    // Remove list from RMS
        //}

        // RMS -> SQL 

        //var rmsTickets = rules.GetModifiedTicketsInRms(lastSyncTimestamp);

        //if (rules.existsNewRmsTickets(sqlTickets, rmsTickets))
        //{
        //    // Add new RMS tickets in SQL
        //}

        //if (rules.existsUpdatedRmsTickets(sqlTickets, rmsTickets))
        //{
        //    // Get tickets with changes from RMS (identify via business fields or timestamps)
        //    // Update discovered tickets list in SQL
        //}

        //if (rules.existsRemovedRmsTicketsInSql(sqlTickets, rmsTickets))
        //{
        //    // Calculate tickets to remove (sqlTickets - rmsTickets)
        //    // Remove list from SQL
        //}

        #endregion
    }
}
