/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package app.metatron.discovery.prep.spark.rule;

import app.metatron.discovery.prep.parser.preparation.rule.expr.Constant;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Constant.ArrayExpr;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Constant.StringExpr;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expr;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expr.BinaryNumericOpExprBase;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expr.FunctionArrayExpr;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expr.FunctionExpr;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expr.UnaryMinusExpr;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expr.UnaryNotExpr;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expression;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Identifier;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Identifier.IdentifierArrayExpr;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Identifier.IdentifierExpr;
import app.metatron.discovery.prep.parser.preparation.rule.expr.RegularExpr;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public class PrepRule {

  List<String> relatedColNames;

  public PrepRule() {
    relatedColNames = new ArrayList();
  }

  public List<String> getIdentifierList(Expression expr) {
    List<String> arr = new ArrayList();

    if (expr instanceof IdentifierExpr) {
      String colName = ((IdentifierExpr) expr).getValue();
      arr.add(colName);
      relatedColNames.add(colName);
    } else {
      for (String colName : ((IdentifierArrayExpr) expr).getValue()) {
        arr.add(colName);
        relatedColNames.add(colName);
      }
    }
    return arr;
  }

  public List<String> getStringList(Expression expr) {
    List<String> arr = new ArrayList();

    if (expr instanceof StringExpr) {
      String colName = StringUtils.strip(expr.toString(), "'");
      arr.add(colName);
      relatedColNames.add(colName);
    } else {
      for (Object obj : ((ArrayExpr) expr).getValue()) {
        String colName = StringUtils.strip((String) obj, "'");
        arr.add(colName);
        relatedColNames.add(colName);
      }
    }
    return arr;
  }

  public static String asSparkExpr(String expr) {
    return expr.replace("==", "=").replace("||", "OR").replace("&&", "AND");
  }

  public class StrExpResult {

    public String str;
    public List<String> arrStr;
    public List<String> identifiers;    // for derive rule

    public StrExpResult() {
      identifiers = new ArrayList();
    }

    public StrExpResult(String str) {
      this(str, Arrays.asList(str));
    }

    public StrExpResult(String str, List<String> arrStr) {
      this.str = str;
      this.arrStr = arrStr;
    }

    public void addIdentifier(String identifier) {
      identifiers.add(identifier);
    }

    public int getArrSize() {
      return arrStr.size();
    }

    public String toColList() {
      if (arrStr.size() >= 3) {
        return arrStr.size() + " columns";
      }

      return str;
    }
  }

  private String wrapIdentifier(String identifier) {
    if (!identifier.matches("[_a-zA-Z\u0080-\uFFFF]+[_a-zA-Z0-9\u0080-\uFFFF]*")) {  // if has odd characters
      return "`" + identifier + "`";
    }
    return identifier;
  }

  private StrExpResult wrapIdentifier(StrExpResult strExpResult) {
    for (int i = 0; i < strExpResult.arrStr.size(); i++) {
      strExpResult.arrStr.set(i, wrapIdentifier(strExpResult.arrStr.get(i)));
    }
    strExpResult.str = joinWithComma(strExpResult.arrStr);
    return strExpResult;
  }

  private String stringifyFuncExpr(FunctionExpr funcExpr) {
    List<Expr> args = funcExpr.getArgs();
    String str = funcExpr.getName() + "(";

    for (Expr arg : args) {
      str += stringifyExpr(arg).str + ", ";
    }
    return str.substring(0, str.length() - 2) + ")";
  }

  private String joinWithComma(List<String> strs) {
    String resultStr = "";

    for (String str : strs) {
      resultStr += str + ", ";
    }
    return resultStr.substring(0, resultStr.length() - 2);
  }

  protected StrExpResult stringifyExpr(Expression expr) {
    if (expr == null) {
      return null;
    }

    StrExpResult result = new StrExpResult();

    if (expr instanceof IdentifierArrayExpr) {    // This should come first because this is the sub-class of Identifier
      IdentifierArrayExpr arrExpr = (IdentifierArrayExpr) expr;
      List<String> wrappedIdentifiers = new ArrayList();

      for (String colName : arrExpr.getValue()) {
        wrappedIdentifiers.add(wrapIdentifier(colName));
        relatedColNames.add(colName);
      }

      result.str = joinWithComma(wrappedIdentifiers);
      result.arrStr = wrappedIdentifiers;
      return result;
    } else if (expr instanceof Identifier) {
      String colName = expr.toString();
      relatedColNames.add(colName);
      return new StrExpResult(wrapIdentifier(colName));
    } else if (expr instanceof FunctionExpr) {
      return new StrExpResult(stringifyFuncExpr((FunctionExpr) expr));
    } else if (expr instanceof FunctionArrayExpr) {
      FunctionArrayExpr funcArrExpr = (FunctionArrayExpr) expr;
      List<String> funcStrExprs = new ArrayList();

      for (FunctionExpr funcExpr : funcArrExpr.getFunctions()) {
        funcStrExprs.add(stringifyFuncExpr(funcExpr));
      }
      return new StrExpResult(joinWithComma(funcStrExprs), funcStrExprs);
    } else if (expr instanceof BinaryNumericOpExprBase) {
      BinaryNumericOpExprBase binExpr = (BinaryNumericOpExprBase) expr;
      return new StrExpResult(
              stringifyExpr(binExpr.getLeft()).str + " " + binExpr.getOp() + " " + stringifyExpr(
                      binExpr.getRight()).str);
    } else if (expr instanceof UnaryNotExpr) {
      UnaryNotExpr notExpr = (UnaryNotExpr) expr;
      return new StrExpResult("!" + stringifyExpr(notExpr.getChild()));
    } else if (expr instanceof UnaryMinusExpr) {
      UnaryMinusExpr minusExpr = (UnaryMinusExpr) expr;
      return new StrExpResult(minusExpr.toString());
    } else if (expr instanceof ArrayExpr) {
      List<String> arrStr = ((ArrayExpr) expr).getValue();
      return new StrExpResult(joinWithComma(arrStr), arrStr);
    }

    return new StrExpResult(expr.toString());
  }

  private static String disableRegexSymbols(String str) {
    String regExSymbols = "[\\<\\(\\[\\{\\\\\\^\\-\\=\\$\\!\\|\\]\\}\\)\\?\\*\\+\\.\\>]";
    return str.replaceAll(regExSymbols, "\\\\$0");
  }

  private static String makeCaseInsensitive(String str) {
    String ignorePatternStr = "";
    for (int i = 0; i < str.length(); i++) {
      String c = String.valueOf(str.charAt(i));

      if (c.matches("[a-zA-Z]")) {
        ignorePatternStr += "[" + c.toUpperCase() + c.toLowerCase() + "]";
      } else {
        ignorePatternStr += c;
      }
    }
    return ignorePatternStr;
  }

  public static String modifyPatternStrWithQuote(String pattern, String quote) {
    if (quote.isEmpty()) {
      return pattern;
    }
    return String.format("%s(?=([^%s]*%s[^%s]*%s)*[^%s]*$)", pattern, quote, quote, quote, quote, quote);
  }

  public static String getPatternStr(Expression expr, Boolean ignoreCase) {
    String patternStr;

    if (expr instanceof Constant.StringExpr) {
      patternStr = ((Constant.StringExpr) expr).getEscapedValue();
      patternStr = disableRegexSymbols(patternStr);

      if (ignoreCase != null && ignoreCase) {
        patternStr = makeCaseInsensitive(patternStr);
      }
    } else if (expr instanceof RegularExpr) {
      patternStr = ((RegularExpr) expr).getEscapedValue();
    } else {
      throw new IllegalArgumentException("illegal pattern type: " + expr);
    }

    return patternStr;
  }

  public static String getQuoteStr(Expression quote) {
    if (quote == null) {
      return "";
    } else {
      assert quote instanceof StringExpr : quote;
      return ((StringExpr) quote).getEscapedValue();
    }
  }

  public static int getLastColno(List<String> targetColNames, List<String> colNames) {
    int lastColno = -1;
    for (int i = 0; i < colNames.size(); i++) {
      if (targetColNames.contains(colNames.get(i))) {
        lastColno = i;
      }
    }
    assert lastColno >= 0 : String.format("targetColNames=[%s] colNames=[%s]", targetColNames, colNames);
    return lastColno;
  }

  public static int getLastColno(List<String> targetColNames, String[] colNames) {
    return getLastColno(targetColNames, Arrays.asList(colNames));
  }

  public String stripQuotes(String str) {
    assert str != null;
    if (str.startsWith("'") && str.endsWith("'")) {
      return str.substring(1, str.length() - 1);
    }
    return str;
  }
}
