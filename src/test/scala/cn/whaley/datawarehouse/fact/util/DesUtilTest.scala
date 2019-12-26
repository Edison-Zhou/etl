package cn.whaley.datawarehouse.fact.util

import cn.whaley.datawarehouse.util.DesUtil
import org.junit.Test

class DesUtilTest {

  @Test
  def test_decrypt(): Unit ={
    val str = "9faa863054f30c2fa9a85a9de4f9de23434d7bcc0b1a063c96061d714ddd9a06906890b5ab14bc341082df8463dcc7d6ed2d1f0a89d376bf9b820ac997b8c5a736262aa82fb7d132eeea71d3bbf5e687929ea00a65c82211640d45541e6c63549654ff285d7bac2482a7f651f2cc230f014acdc61cb8f7427077610326e32fc7356f796fdb38c0e447e8b2d10aac7635c67b3a556905d80b094ad54e9c7e43176b89699b302f9b648f5583e7d348da6b3f54a20933488c64f8bc29a666b79de64c15aa124fc52f02e224f5b3c5ab281109a0819c0dc21e49bf54fcb46d548c41d1c5f8c2da99330eec1acea0ff51dc6a8987af3a445cb06ab40afbeaa6752797f37712fe714da3d482da5bebe95578f05cb9efe3408b751466d43eb8138a577f3bb76f5948badc6e7fbd269a31c7fd7eb732ec8b1f76affcf4ad3080ecf52a5629aaeae826f0687e398604145e8c88d18a0c7da58fd5a2bf12e361469e97b2ce3b94ae6046844c3b2a591202a5305d8741da71c1aa197a35f85c37d1cf2c3650d54129098e892e41ebcfc33a1cb72366d1fad06561b4b159e1b2ec0b955bd8394c0b1b3b8633da746da040e7deb72831048849fbd965507bbed3c78289bdeca40901d30dde352f7a094ad54e9c7e4317ef8cadf8d0ac689ce3ea8b9033668f44da32f9aa2577f3f9b3be0eded651f9611c03de06678bc467fe705c9bb228f40df54d91dd694f931add313e453080516af7d5b0527cc6908521e4a2116759e96c45159e95d596e2b3be7241f1e887d23339c277a6a823fed1b461b26b3f45cff92b00593e1db52d5f0f5131b197c7f827e7bb330509a0fabd78637e082ff9242bd2e43110a33f615b"
    println(DesUtil.decrypt(str))
  }
}