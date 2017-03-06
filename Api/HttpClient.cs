/*
// $Id: WebFetch.cs 4386 2009-07-07 11:25:15Z MOBEO\Fernando Marques Lima $
//
// Revision      : $Revision: 4386 $
// Modified Date : $LastChangedDate: 2010-02-15 12:30:00 +0100 (ter, 16 Fev 2010) $ 
// Modified By   : $LastChangedBy: MOBEO\Fernando Marques Lima $
// 
// (c) Copyright 2010 Mobeo Lda
*/
using System;
using System.IO;
using System.Net;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Security.Cryptography.X509Certificates;
using System.Collections.Specialized;
using Microsoft.Win32;
using System.Globalization;

namespace WestPakMiddleware.Api {
    public sealed class HttpClient {
        public static string TryGet(string url) {
            try {
                return Get(url);
            } catch (Exception ex) {
                return null;
            }
        }

        public static MemoryStream DownloadFile(string url) {
            var client = new WebClient();

            try {
                return new MemoryStream(client.DownloadData(url));
            } finally {
                client.Dispose();
            }

            return null;
        }

        public static HttpWebResponse GetFileHead(string url) {
            var request = (HttpWebRequest) WebRequest.Create(url);
            request.Method = "HEAD";

            return (HttpWebResponse) (request.GetResponse());
        }

        public static string GetWithEncoding(string url, Encoding encoding) {
            var request = WebRequest.Create(url);
            var response = (HttpWebResponse) request.GetResponse();
            var stream = response.GetResponseStream();
            var reader = new StreamReader(stream, encoding);

            var result = reader.ReadToEnd();

            reader.Close();
            stream.Close();
            response.Close();

            return result;
        }

        public static string GetWithAutomaticEncoding(string url) {
            var request = WebRequest.Create(url);
            var response = (HttpWebResponse) request.GetResponse();
            var stream = response.GetResponseStream();
            var reader = new StreamReader(stream, true);

            var result = reader.ReadToEnd();

            reader.Close();
            stream.Close();
            response.Close();

            return result;
        }

        public static string Get2(string url) {
            var request = WebRequest.Create(url);
            var response = (HttpWebResponse) request.GetResponse();
            var stream = response.GetResponseStream();
            var reader = new StreamReader(stream, System.Text.Encoding.GetEncoding("ISO-8859-1"));
            
            var result = reader.ReadToEnd();

            reader.Close();
            stream.Close();
            response.Close();

            return result;
        }

        public static string Get(string url) {
            System.Net.ServicePointManager.CertificatePolicy = new MyPolicy(); // TODO

            var request = WebRequest.Create(url);

            HttpWebResponse response;

            try {
                response = (HttpWebResponse) request.GetResponse();
            } catch (WebException ex) {
                response = (HttpWebResponse) ex.Response;
            }

            var stream = response.GetResponseStream();
            var reader = new StreamReader(stream, true);
            var statusCode = (int) ((HttpWebResponse) response).StatusCode;
            var statusDesc = ((HttpWebResponse) response).StatusDescription;

            var result = reader.ReadToEnd();
            var resultDetail = "HTTP code: " + statusCode + " " + statusDesc + Environment.NewLine + "Response: " + reader.ReadToEnd();

            reader.Close();
            stream.Close();
            response.Close();

            if (statusCode == 200)
                return result;
            else
                throw new Exception(resultDetail);
        }

        public static string TryPost(string url, string contentType, string content) {
            return TryPost(url, contentType, content, null, null);
        }

        public static string TryPost(string url, string contentType, string content, string username, string password) {
            try {
                return Post(url, contentType, content, username, password);
            } catch (Exception ex) {
                return null;
            }
        }

        public static string Post(string url, string contentType, string content) {
            return Post(url, contentType, content, null, null);
        }

        public static string Post(string url, string contentType, string content, string username, string password) {
            var buffer = Encoding.ASCII.GetBytes(content);

            var request = (HttpWebRequest) WebRequest.Create(url);
            request.Method = "POST";
            request.ContentType = contentType;
            request.ContentLength = buffer.Length;

            if (username != null)
                request.Credentials = new NetworkCredential(username, password);

            var post = request.GetRequestStream();
            post.Write(buffer, 0, buffer.Length);
            post.Close();

            return new StreamReader(((HttpWebResponse) request.GetResponse()).GetResponseStream()).ReadToEnd(); 
        }

        public static string JsonPost(string uri, string parameters) {
            var webRequest = WebRequest.Create(uri);

            webRequest.ContentType = "application/json";
            webRequest.Method = "POST";

            byte[] bytes = Encoding.ASCII.GetBytes(parameters);
            Stream os = null;

            try {
                webRequest.ContentLength = bytes.Length;
                os = webRequest.GetRequestStream();
                os.Write(bytes, 0, bytes.Length);
            } catch (WebException ex) {
                var error1 = ex.StackTrace; // HttpPost: Request error
            } finally {
                if (os != null)
                    os.Close();
            }

            try {
                var webResponse = webRequest.GetResponse();

                if (webResponse == null)
                    return null;

                var sr = new StreamReader(webResponse.GetResponseStream());

                return sr.ReadToEnd().Trim();
            } catch (WebException ex) {
                try {
                    var sr = new StreamReader(ex.Response.GetResponseStream());
                    var resultError = sr.ReadToEnd();

                    Console.WriteLine(resultError);
                } catch (Exception ex2) {
                    var error3 = ex2.StackTrace;
                }

                var error2 = ex.StackTrace; // HttpPost: Request error
            }

            return null;
        }

        public static string UploadFormFile(string url, string filename, string fileFieldName, Stream stream) {
            var headers = new NameValueCollection();
            headers.Add("x-headerName", "header value");

            var response = PostFile(new Uri(url), null, stream, filename, null, fileFieldName, new CookieContainer(), headers);

            if (response != null) {
                var reader = new StreamReader(response.GetResponseStream());
                var statusCode = (int) ((HttpWebResponse) response).StatusCode;
                var statusDesc = ((HttpWebResponse) response).StatusDescription;

                if (statusCode != 200)
                    throw new Exception("HTTP code: " + statusCode + " " + statusDesc + Environment.NewLine + "Response: " + reader.ReadToEnd());

                if (reader != null)
                    return reader.ReadToEnd();
            }

            return null;
        }

        public static string AsmxWebserviceSoapPost(string url, string soapBody) {
            var buffer = Encoding.ASCII.GetBytes(soapBody);

            var request = (HttpWebRequest) WebRequest.Create(url);
            request.Method = "POST";
            request.ContentType = "text/xml"; //"application/x-www-form-urlencoded";
            request.ContentLength = buffer.Length;
            
            var post = request.GetRequestStream();
            post.Write(buffer, 0, buffer.Length);
            post.Close();

            return new StreamReader(((HttpWebResponse) request.GetResponse()).GetResponseStream()).ReadToEnd(); 
        }

        public class MyPolicy: ICertificatePolicy {
            public bool CheckValidationResult(ServicePoint srvPoint, X509Certificate certificate, WebRequest request, int certificateProblem) {
                return true;
            }
        }

        // Helpers

        public static WebResponse PostFile(Uri requestUri, NameValueCollection postData, string fileName, string fileContentType, string fileFieldName, CookieContainer cookies, NameValueCollection headers) {
            using (FileStream fileData = File.Open(fileName, FileMode.Open, FileAccess.Read, FileShare.Read)) {
                return PostFile(requestUri, postData, fileData, fileName, fileContentType, fileFieldName, cookies, headers);
            }
        }

        public static WebResponse PostFile(Uri requestUri, NameValueCollection postData, Stream fileData, string fileName, string fileContentType, string fileFieldName, CookieContainer cookies, NameValueCollection headers) {
            var webrequest = (HttpWebRequest) WebRequest.Create(requestUri);

            string ctype;

            fileContentType = string.IsNullOrEmpty(fileContentType) ?
                TryGetContentType(fileName, out ctype) ?
                    ctype : "application/octet-stream" : fileContentType;

            fileFieldName = string.IsNullOrEmpty(fileFieldName) ? "file" : fileFieldName;

            if (headers != null) {
                foreach (string key in headers.AllKeys) { // set the headers
                    var values = headers.GetValues(key);

                    if (values != null)
                        foreach (string value in values) {
                            webrequest.Headers.Add(key, value);
                        }
                }
            }

            webrequest.Method = "POST";

            if (cookies != null) {
                webrequest.CookieContainer = cookies;
            }

            var boundary = "----------" + DateTime.Now.Ticks.ToString("x", CultureInfo.InvariantCulture);

            webrequest.ContentType = "multipart/form-data; boundary=" + boundary;
            StringBuilder sbHeader = new StringBuilder();

            // add form fields, if any

            if (postData != null) {
                foreach (string key in postData.AllKeys) {
                    string[] values = postData.GetValues(key);
                    if (values != null)
                        foreach (string value in values) {
                            sbHeader.AppendFormat("--{0}\r\n", boundary);
                            sbHeader.AppendFormat("Content-Disposition: form-data; name=\"{0}\";\r\n\r\n{1}\r\n", key,
                                                    value);
                        }
                }
            }

            if (fileData != null) {
                sbHeader.AppendFormat("--{0}\r\n", boundary);
                sbHeader.AppendFormat("Content-Disposition: form-data; name=\"{0}\"; {1}\r\n", fileFieldName,
                                        string.IsNullOrEmpty(fileName)
                                            ?
                                                ""
                                            : string.Format(CultureInfo.InvariantCulture, "filename=\"{0}\";",
                                                            Path.GetFileName(fileName)));

                sbHeader.AppendFormat("Content-Type: {0}\r\n\r\n", fileContentType);
            }

            byte[] header = Encoding.UTF8.GetBytes(sbHeader.ToString());
            byte[] footer = Encoding.ASCII.GetBytes("\r\n--" + boundary + "--\r\n");
            long contentLength = header.Length + (fileData != null ? fileData.Length : 0) + footer.Length;

            webrequest.ContentLength = contentLength;

            using (Stream requestStream = webrequest.GetRequestStream()) {
                requestStream.Write(header, 0, header.Length);

                if (fileData != null) {
                    // Write the file data, if any

                    byte[] buffer = new Byte[checked((uint) Math.Min(4096, (int) fileData.Length))];
                    int bytesRead;

                    while ((bytesRead = fileData.Read(buffer, 0, buffer.Length)) != 0)
                        requestStream.Write(buffer, 0, bytesRead);
                }

                // Write footer

                requestStream.Write(footer, 0, footer.Length);

                try {
                    return webrequest.GetResponse();
                } catch (WebException ex) {
                    return ex.Response;
                }
            }
        }

        public static bool TryGetContentType(string fileName, out string contentType) {
            try {
                var key = Registry.ClassesRoot.OpenSubKey(@"MIME\Database\Content Type");

                if (key != null) {
                    foreach (string keyName in key.GetSubKeyNames()) {
                        var subKey = key.OpenSubKey(keyName);

                        if (subKey != null) {
                            var subKeyValue = (string) subKey.GetValue("Extension");

                            if (!string.IsNullOrEmpty(subKeyValue)) {
                                if (string.Compare(Path.GetExtension(fileName).ToUpperInvariant(), subKeyValue.ToUpperInvariant(), StringComparison.OrdinalIgnoreCase) == 0) {
                                    contentType = keyName;
                                    return true;
                                }
                            }
                        }
                    }
                }
            } catch {

            }

            contentType = "";
            return false;
        }
    }
}
