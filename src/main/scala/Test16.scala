import java.net.URLEncoder

import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod
object Test16 {
  def main(args: Array[String]): Unit = {
    val client = new HttpClient
    // 设置代理服务器地址和端口
    //把中文转成Unicode编码
    // 使用 GET 方法 ，如果服务器需要通过 HTTPS 连接，那只需要将下面 URL 中的 http 换成 https
    val method = new GetMethod("http://fundtest.eastmoney.com/dataapi1015/ztjj//GetBKListByBKType?callback=jQuery1830552021903065689_1605938825254&_=1605938825282")
//    val method = new GetMethod("http://push2.eastmoney.com/api/qt/stock/get?ut=b4204f1133ba85d41936008f61488734&fltt=2&fields=f57,f58,f86,f170&secid=90.BK0478")
    client.executeMethod(method)
    //打印服务器返回的状态
    println(method.getStatusLine)
    //打印返回的信息
    println(method.getResponseBodyAsString)
    //释放连接
    method.releaseConnection()
  }

}
