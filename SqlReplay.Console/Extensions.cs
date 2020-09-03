using System.Text.RegularExpressions;
using System.Xml;

namespace SqlReplay.Console
{
    using System;
    using System.Text;

    public static class Extensions
    {
        public static string GetParenthesesContent(this string text)
        {
            return GetParenthesesContent(text, text.IndexOf('('));
        }

        public static string GetParenthesesContent(this string text, out bool skipConvert)
        {
            return text.GetParenthesesContentForXmlSqlDbType(text.IndexOf('('), out skipConvert);
        }

        public static string GetParenthesesContent(this string text, int leftParenthesisIndex)
        {
            char[] chars = text.ToCharArray();
            int lefts = 1;
            int rights = 0;
            for (int i = leftParenthesisIndex + 1; i < chars.Length; ++i)
            {
                if (chars[i] == '(')
                {
                    lefts++;
                }
                else if (chars[i] == ')')
                {
                    if (++rights == lefts)
                    {
                        return text.Substring(leftParenthesisIndex + 1, i - (leftParenthesisIndex + 1));
                    }
                }
            }
            throw new Exception("Text does not have matching parentheses.");
        }

        public static string GetParenthesesContentForXmlSqlDbType(this string text, int leftParenthesisIndex, out bool skipConvert)
        {
            string res;
            skipConvert = false;

            try
            {
                res = text.GetParenthesesContent(leftParenthesisIndex);
            }
            catch
            {
                string startReplace = @"(xml,N'<";
                string startReplaceWith = "<";

                res = text.Substring(leftParenthesisIndex).Replace(startReplace, startReplaceWith);

                string rootStart = res.Substring(0, res.IndexOf('>') + 1);
                string rootEnd = "</" + rootStart.Substring(1);

                string endReplace = $"{rootEnd}')";
                string endReplaceWith = rootEnd;

                res = res.Substring(0,
                    res.IndexOf(endReplace, StringComparison.CurrentCulture) + endReplaceWith.Length);

                if (!string.IsNullOrWhiteSpace(res) && res.IsValidXml())
                {
                    skipConvert = true;
                }
                else
                {
                    throw new Exception("Text does not have matching parentheses.");
                }
            }
            return res;
        }

        public static bool IsValidXml(this string xmlString)
        {
            Regex tagsWithData = new Regex("<\\w+>[^<]+</\\w+>");

            if (string.IsNullOrEmpty(xmlString) || tagsWithData.IsMatch(xmlString) == false)
            {
                return false;
            }

            try
            {
                XmlDocument xmlDocument = new XmlDocument();
                xmlDocument.LoadXml(xmlString);
                return true;
            }
            catch
            {
                return false;
            }
        }
    }
}
