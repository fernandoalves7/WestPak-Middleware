using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.IO;

namespace WestPakMiddleware.Api
{
    public sealed class Settings
    {
        private Configuration configuration;

        // Construction

        public Settings(string orgName, string appName): this(orgName, appName, null)
        {

        }

        public Settings(string orgName, string appName, string defaultXmlContent)
        {
            //var settingsDir = System.Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData) + @"\" + orgName;
            var settingsDir = System.Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData) + @"\" + orgName;
            var settingsPath = settingsDir + @"\" + appName + ".exe.config";

            if (!Directory.Exists(settingsDir))
                Directory.CreateDirectory(settingsDir);

            if (!File.Exists(settingsPath))
            {
                if (string.IsNullOrWhiteSpace(defaultXmlContent))
                    throw new Exception("Unable to create settings file since there is no default content");

                File.WriteAllText(settingsPath, defaultXmlContent);
            }

            configuration = ConfigurationManager.OpenMappedExeConfiguration(new ExeConfigurationFileMap()
            {
                ExeConfigFilename = settingsPath
            }, ConfigurationUserLevel.None);

            var path = configuration.FilePath;
        }

        // Public methods

        public string GetLowerCase(string key)
        {
            var result = Get(key);

            return result != null ?
                result.ToLower() : null;
        }

        public string Get(string key)
        {
            if (configuration == null || configuration.AppSettings == null ||
                configuration.AppSettings.Settings == null || configuration.AppSettings.Settings[key] == null)
                return null;

            return configuration.AppSettings.Settings[key].Value;
        }

        public long GetAsInt64(string key) {
            var value = Get(key);

            if (string.IsNullOrWhiteSpace(value))
                return 0;

            return Convert.ToInt64(value);
        }

        public long GetAsInt64(string key, int defaultValue) {
            try {
                var value = Get(key);

                if (string.IsNullOrWhiteSpace(value))
                    return defaultValue;

                return Convert.ToInt64(value);
            } catch (Exception ex) {
                return defaultValue;
            }
        }

        public int GetAsInt32(string key) {
            var value = Get(key);

            if (string.IsNullOrWhiteSpace(value))
                return 0;

            return Convert.ToInt32(value);
        }

        public int GetAsInt32(string key, int defaultValue)
        {
            try
            {
                var value = Get(key);

                if (string.IsNullOrWhiteSpace(value))
                    return defaultValue;

                return Convert.ToInt32(value);
            }
            catch (Exception ex)
            {
                return defaultValue;
            }
        }

        public bool GetAsBoolean(string key, bool defaultValue)
        {
            try
            {
                var value = GetLowerCase(key);

                if (string.IsNullOrWhiteSpace(value))
                    return defaultValue;

                return value == "true" || value == "t" || value == "1";
            }
            catch (Exception ex)
            {
                return defaultValue;
            }
        }

        public bool GetAsBoolean(string key)
        {
            var value = GetLowerCase(key);

            if (string.IsNullOrWhiteSpace(value))
                return false;

            return value == "true" || value == "t" || value == "1";
        }

        public DateTime? GetAsDateTime(string key)
        {
            var datetime = Get(key);

            if (string.IsNullOrWhiteSpace(datetime))
                return null;

            var d = (from x in datetime.Split(' ')
                     select Convert.ToInt32(x)).ToList();

            return new DateTime(d[0], d[1], d[2], d[3], d[4], d[5]);
        }

        public void Set(string key, string value)
        {
            if (!configuration.AppSettings.Settings.AllKeys.Contains(key))
            {
                configuration.AppSettings.Settings.Add(key, "");
                configuration.Save();
            }

            configuration.AppSettings.Settings[key].Value = value;
            configuration.Save();
        }

        public void Set(string key, long value) {
            var valueStr = value.ToString();
            Set(key, valueStr);
        }

        public void Set(string key, DateTime? value) {
            if (value == null)
                Set(key, "");
            else
                Set(key, value.Value.ToString("yyyy MM dd HH mm ss"));
        }

        public static string GetSettingsFile(string orgName, string appName)
        {
            return System.Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData) + @"\" + orgName + @"\" + appName + ".exe.config";
        }

        public static string GetSettingsFolder(string orgName)
        {
            return System.Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData) + @"\" + orgName;
        }

        public static void DeleteSettingsFile(string orgName, string appName)
        {
            //var settingsDir = System.Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData) + @"\" + AppOrg;
            var settingsDir = System.Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData) + @"\" + orgName;
            var settingsPath = settingsDir + @"\" + appName + ".exe.config";

            if (System.IO.Directory.Exists(settingsDir) && System.IO.File.Exists(settingsPath))
                try
                {
                    System.IO.File.Delete(settingsPath);
                }
                catch (Exception ex)
                {

                }
        }
    }
}
