package test3

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object Test18 {
  var map = Map(
    "高速公路" -> "gsgl",
    "交运物流" -> "jywl",
    "水泥建材" -> "snjc",
    "工程建设" -> "gcjs",
    "公用事业" -> "gysy",
    "电力行业" -> "dlhy",
    "交运设备" -> "jysb",
    "农牧饲渔" -> "nmsy",
    "煤炭采选" -> "mtcx",
    "食品饮料" -> "spyl",
    "电子信息" -> "dzxx",
    "通讯行业" -> "txhy",
    "房地产" -> "fdc",
    "家电行业" -> "jdhy",
    "仪器仪表" -> "yqyb",
    "电子元件" -> "dzyj",
    "石油行业" -> "syhy",
    "医药制造" -> "yyzz",
    "造纸印刷" -> "zzys",
    "券商信托" -> "qsxt",
    "保险" -> "bx",
    "银行" -> "yh",
    "木业家具" -> "myjj",
    "酿酒行业" -> "njhy",
    "有色金属" -> "ysjs",
    "钢铁行业" -> "gthy",
    "航天航空" -> "hthk",
    "汽车行业" -> "qchy",
    "国际贸易" -> "gjmy",
    "文化传媒" -> "whcm",
    "材料行业" -> "clhy",
    "化工行业" -> "hghy",
    "综合行业" -> "zhhy",
    "机械行业" -> "jxhy",
    "玻璃陶瓷" -> "bltc",
    "园林工程" -> "ylgc",
    "医疗行业" -> "ylhy",
    "环保工程" -> "hbgc",
    "贵金属" -> "gjs",
    "包装材料" -> "bzzcl",
    "软件服务" -> "rjfw",
    "多元金融" -> "dyjr",
    "金属制品" -> "jszp",
    "文教休闲" -> "wjxx",
    "专用设备" -> "zysb",
    "军工" -> "jg",
    "新材料" -> "xcl",
    "基金重仓" -> "jjzc",
    "生物疫苗" -> "swym",
    "锂电池" -> "ldc",
    "LED" -> "led",
    "智能电网" -> "zndw",
    "太阳能" -> "tyn",
    "智慧城市" -> "zhcs",
    "北斗导航" -> "bddh",
    "创业成份" -> "cycf",
    "智能机器" -> "znjq",
    "在线教育" -> "zxjy",
    "医疗器械" -> "ylqx",
    "蓝宝石" -> "lbs",
    "燃料电池" -> "rldc",
    "基因测序" -> "jycx",
    "国产软件" -> "gcrj",
    "免疫治疗" -> "myzl",
    "无人机" -> "wrj",
    "5G概念" -> "5ggn",
    "人工智能" -> "rgzn",
    "增强现实" -> "zqxs",
    "军民融合" -> "jmrh",
    "OLED" -> "oled",
    "体外诊断" -> "twzd",
    "华为概念" -> "hwgn",
    "边缘计算" -> "byjs",
    "氢能源" -> "qny",
    "ETC" -> "etc",
    "新能源车" -> "xnyc",
    "云游戏" -> "yyx"
  )

  def main(args: Array[String]): Unit = {
    val client = new HttpClient
    //    val method = new GetMethod("https://fundtest.eastmoney.com/dataapi1015/ztjj//GetBKListByBKType?callback=jQuery1830552021903065689_1605938825254&_=1605938825282")
    val method = new GetMethod("http://fundtest.eastmoney.com/dataapi1015/ztjj//GetBKListByBKType?callback=jQuery183049381993207298436_1605775126000&_=1605775126000")
    //    val method = new GetMethod("http://push2.eastmoney.com/api/qt/stock/get?ut=b4204f1133ba85d41936008f61488734&fltt=2&fields=f57,f58,f86,f170&secid=90.BK0478")
    method.addRequestHeader("Accept", "*/*")
    method.addRequestHeader("Accept-Encoding", "gzip, deflate")
    method.addRequestHeader("Accept-Language", "zh-CN,zh;q=0.9")
    method.addRequestHeader("Host", "fundtest.eastmoney.com")
    method.addRequestHeader("Referer", "http://fund.eastmoney.com/")
    method.addRequestHeader("User-Agent", "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.183 Safari/537.36")
    client.executeMethod(method)

    var arr = new ArrayBuffer[(String, Double)]
    val resString = method.getResponseBodyAsString
    val resJson = resString.substring(resString.indexOf("(") + 1, resString.lastIndexOf(")"))
    method.releaseConnection()
    var line: JSONObject = null
    for (theme <- JSON.parseObject(resJson).getJSONObject("Data").asScala) {
      for (elem <- theme._2.asInstanceOf[JSONArray].asScala) {
        line = elem.asInstanceOf[JSONObject]
        //        println(line.getString("BKName"), line.getString("ZDF"))
        arr += ((line.getString("BKName") -> line.getString("ZDF").toDouble))
      }
    }
    arr.sortBy(_._1).reverse.foreach(println(_))
  }
}
