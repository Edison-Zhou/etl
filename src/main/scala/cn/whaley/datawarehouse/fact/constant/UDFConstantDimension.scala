package cn.whaley.datawarehouse.fact.constant

/**
  * Created by baozhi.wang on 2017/3/24.
  * 定义UDF中所需要的常量
  */
object UDFConstantDimension {

  /**
    * For PathParser
    * *******************************************************************
    */
  // 定义日志类型：logType
  val INTERVIEW = "interview"
  val DETAIL = "detail"
  val PLAYVIEW = "playview" //(注：对于medusa而言，为play日志；对于moretv而言，为playview日志)
  val PLAY = "play" // For medusa

  // 定义路径字段类型：pathType
  val PATH = "path" // For moretv
  val PATHMAIN = "pathMain" // For medusa
  val PATHSUB = "pathSub" // For medusa
  val PATHSPECIAL = "pathSpecial" // For medusa

  // 定义输出类型：outpType
  val CONTENTTYPE = "contentType" // For moretv

  // pathSpecial
  val PATHPROPERTY = "pathProperty" // For medusa,从pathSpecial中获取
  val PATHIDENTIFICATION = "pathIdentification" // For medusa,从pathSpecial中获取
  // pathSub
  val ACCESSPATH = "accessPath" // For medusa,从pathSub中获取
  val PREVIOUSSID = "previousSid" // For medusa,从pathSub中获取
  val PREVIOUSCONTENTTYPE = "previousContentType" // For medusa,从pathSub中获取
  // pathMain
  val LAUNCHERAREA = "launcherArea" // For medusa,从pathMain中获取
  val LAUNCHERACCESSLOCATION = "launcherAccessLocation" // For medusa,从pathMain中获取
  val PAGETYPE = "pageType" // For medusa,从pathMain中获取
  val PAGEDETAILINFO = "pageDetailInfo" // For medusa,从pathMain中获取


  /*------------------------------------------add by michael start------------------------------------------*/
  // pathMain from medusa
  val RETRIEVAL_DIMENSION = "retrieval"
  val RETRIEVAL_DIMENSION_CHINESE = "筛选"

  //path from moretv
  val MULTI_SEARCH = "multi_search"

  val SEARCH_DIMENSION = "search"
  val SEARCH_DIMENSION_CHINESE = "搜索"

  val HOME_SEARCH = "home-search"


  //used for 列表页过滤
  val HORIZONTAL = "horizontal"
  val MV_RECOMMEND_HOME_PAGE = "mvRecommendHomePage"
  val MV_TOP_HOME_PAGE = "mvTopHomePage"

  //用来获取列表页使用
  val HOME_CLASSIFICATION = "home*classification"
  val HOME_MY_TV = "home*my_tv"
  //小鹰直播
  val HOME_LIVE_EAGLE = "home*live*eagle"

  /** 用于频道分类入口统计，解析出资讯的一级入口、二级入口
    * 从今日推荐入口【位于分类的上面】里面的位置的最右下角落进入咨询短片频道的
    * home*recommendation*1-hot*五花八门 */
  val HOME_RECOMMENDATION = "home*recommendation"

  val KIDS_HOME = "kids_home-"
  val KIDS = "kids"
  val INTEREST_INTEREST = "interest-interest"
  val INTEREST_HOME = "interest-home"
  val HOME_HOTSUBJECT = "home*hotSubject" //首页短视频区域，按照编辑的逻辑全部归为奇趣频道
  val HOT_HOT = "hot-hot*分类入口"
  val HOT_HOME = "hot-home*分类入口"
  val GAME_GAME = "game-game"
  val COLLECT = "collect"
  val ACCOUNTCENTER_HOME = "accountcenter_home"
  val SPORTS_LIST_DIMENSION_TRAIT = "sports*"
  val MV_CATEGORY = "mv_category"
  val MV_POSTER = "mv_poster"

  val MEDUSA_LIST_Page_LEVEL_1 = Array("movie", "tv", "zongyi", "jilu", "comic", "xiqu", "collect", "accountcenter_home", "account", "hot", "member", "cantonese")
  // MEDUSA_LIST_Page_LEVEL_2 use MedusaPageDetailInfo , not need mv kids and sport in MEDUSA_LIST_Page_LEVEL_1


  val MEDUSA_BIG_FACT_TABLE_DIR = "/log/medusaAndMoretvMergerDimension"
  val MEDUSA_BIG_FACT_TABLE_PLAY_TYPE = "playview2filter"
  val MEDUSA_DATA_WAREHOUSE = "/data_warehouse/dw_fact_medusa"
  val MEDUSA_DAILY_DIMENSION_DATA_WAREHOUSE = "/data_warehouse/dw_dimensions/daily"
  val MEDUSA_DIMENSION_BACKUP_DATA_WAREHOUSE = "/data_warehouse/dw_dimensions/backup"
  val MEDUSA_DIMENSION_DATA_WAREHOUSE = "/data_warehouse/dw_dimensions"
  /*-------------------筛选维度-------------------*/
  //筛选维度表名称
  val SOURCE_RETRIEVAL_TABLE = "dim_medusa_source_retrieval"
  //筛选维度字段【排序方式：最新、最热、得分；标签；地区；年代】
  val FILTER_CATEGORY_1 = "retrieval_sort_way"
  val FILTER_CATEGORY_2 = "retrieval_tag"
  val FILTER_CATEGORY_3 = "retrieval_area"
  val FILTER_CATEGORY_4 = "retrieval_decade"
  val RETRIEVAL_RESULT_INDEX = "retrieval_result_index"
  //维度表主键
  val SOURCE_RETRIEVAL_SK = "source_retrieval_sk"
  //具体字段,用来生成md5
  val SOURCE_RETRIEVAL_COLUMN = "retrieval_sort_way,retrieval_tag,retrieval_area,retrieval_decade,retrieval_result_index"
  /*-------------------筛选维度end-------------------*/

  /*-------------------搜索维度-------------------*/
  //搜索维度表名称
  val SOURCE_SEARCH_TABLE = "dim_medusa_source_search"
  //搜索来源维度字段
  val SEARCH_FROM = "search_from"
  val SEARCH_KEYWORD = "search_keyword"
  val SEARCH_TAB_NAME = "search_tab_name"
  val SEARCH_RESULT_INDEX = "search_result_index"
  val SEARCH_MOST_SEARCH = "search_most_search"


  //维度表主键
  val SOURCE_SEARCH_SK = "source_search_sk"
  //具体字段,用来生成md5
  val SOURCE_SEARCH_COLUMN = "search_from,search_keyword,search_tab_name,search_result_index,search_most_search"
  /*-------------------搜索维度end-------------------*/

  /*-------------------列表页维度-------------------*/
  //列表页维度表名称
  val SOURCE_LIST_TABLE = "dim_medusa_source_list"
  //列表页维度字段
  val MAIN_CATEGORY = "main_category"
  val SUB_CATEGORY = "second_category"
  val THIRD_CATEGORY = "third_category"
  val FOURTH_CATEGORY = "fourth_category"
  val FIFTH_CATEGORY = "fifth_category"
  val SOURCE_LIST_INDEX = "list_index"

  //维度表主键
  val SOURCE_LIST_SK = "source_list_sk"
  //具体字段,用来生成md5
  val SOURCE_LIST_COLUMN = "main_category,second_category,third_category,fourth_category,fifth_category,list_index"
  /*-------------------列表页维度end-------------------*/

  /*-------------------推荐入口维度-------------------*/
  //推荐入口维度表名称
  val SOURCE_RECOMMEND_TABLE = "dim_medusa_source_recommend"
  //推荐入口维度字段
  val RECOMMEND_SOURCE_TYPE = "recommend_source_type" //peoplealsolike,similar,guessyoulike
  val RECOMMEND_PROPERTY = "recommend_property"
  val RECOMMEND_PRE_CONTENT_TYPE = "pre_content_type"
  val RECOMMEND_METHOD = "recommend_method"

  //维度表主键
  val SOURCE_RECOMMEND_SK = "source_recommend_sk"
  //具体字段,用来生成md5
  val SOURCE_RECOMMEND_COLUMN = "recommend_source_type,recommend_property,pre_content_type,recommend_method"
  /*-------------------推荐入口维度end-------------------*/


  /*-------------------特殊入口维度-------------------*/
  //特殊入口维度表名称
  val SOURCE_SPECIAL_TABLE = "dim_medusa_source_special"
  //列表页维度字段
  val SPECIAL_SOURCE_TYPE = "special_type"
  val SPECIAL_SOURCE_ID = "special_id"
  val SPECIAL_SOURCE_NAME = "special_name"
  //维度表主键
  val SOURCE_SPECIAL_SK = "source_special_sk"
  //具体字段,用来生成md5
  val SOURCE_SPECIAL_COLUMN = "if(special_type='',special_type_v2,special_type),if(special_id='',special_id_v2,special_id) ,if(special_name='',special_name_v2,special_name) "
  val SOURCE_SPECIAL_COLUMN_FOR_DIMENSION = "if(special_type='',special_type_v2,special_type) as special_type,if(special_id='',special_id_v2,special_id) as special_id ,if(special_name='',special_name_v2,special_name) as special_name "
  val SOURCE_SPECIAL_COLUMN_NOT_SHOW = "special_type,special_id,special_name,special_type_v2,special_id_v2,special_name_v2"

  /*-------------------特殊入口入口维度end-------------------*/


  /*-------------------首页来源入口入口维度-------------------*/
  //列表页维度表名称
  val SOURCE_LAUNCHER_TABLE = "dim_medusa_source_launcher"
  //列表页维度字段
  val SOURCE_LAUNCHER_AREA = "launcher_area"
  val SOURCE_LAUNCHER_POSITION = "launcher_position"
  val SOURCE_LAUNCHER_POSITION_INDEX = "launcher_position_index"
  //维度表主键
  val SOURCE_LAUNCHER_SK = "source_launcher_sk"
  //具体字段,用来生成md5[节省存储空间，不需要将二选一的值，为null的赋值默认值]
  val SOURCE_LAUNCHER_COLUMN = "launcher_area,launcher_position,launcher_position_index"
  val SOURCE_LAUNCHER_COLUMN_NOT_SHOW = "launcher_area,launcher_position,launcher_position_index"
  /*-------------------首页来源入口入口维度end-------------------*/

  /*-------------------外部维度-------------------*/
  //日期维度
  val DIM_DATE_KEY = "dim_date_key"
  //时间维度
  val DIM_TIME_KEY = "dim_time_key"
  //sid维度
  val DIM_PROGRAM_SK = "dim_program_sk"
  //终端用户维度
  val DIM_TERMINAL_SK = "dim_terminal_sk"
  //账号维度
  val DIM_ACCOUNT_SK = "dim_account_sk"
  //IP地址维度
  val DIM_WEB_LOCATION_SK = "dim_web_location_sk"
  //app version维度
  val DIM_APP_VERSION_KEY = "dim_app_version_key"
  /*-------------------外部维度end-------------------*/

  /*-------------------外部维度表中不需要在事实表展示的字段-------------------*/
  val DIM_APP_VERSION_COLUMN_NOT_SHOW = "buildDate,apkVersion,apkSeries"
  val DIM_MEDUSA_PROGRAM_COLUMN_NOT_SHOW = "videoSid"
  val DIM_MEDUSA_TERMINAL_COLUMN_NOT_SHOW = "userId"


  /*-------------------大宽表中不需要在事实表展示的字段-------------------*/
  val FAT_TABLE_COLUMN_NOT_SHOW = "accountId,accessPathFromPath,versionCode,appEnterWay,ip,launcherAccessLocationFromPath,launcherAreaFromPath,logType,logVersion,omnibusName,omnibusSid,pageDetailInfoFromPath,pageTypeFromPath,path," +
    "pathIdentificationFromPath,pathMain,pathPropertyFromPath,pathSpecial,pathSub,previousContentTypeFromPath,previousSidFromPath,retrieval,searchText," +
    "singer,singerSid,station,subjectCode,subjectName,topRankName,topRankSid,videoSid,date,day,videoName,productModel"
  //exist in table: datetime,duration,episodeSid,event,flag,mark,promotionChannel


  //事实表
  val FACT_MEDUSA_PLAY = "fact_medusa_play"
  //维度表
  val DIM_APP_VERSION_TABLE_NAME = "dim_app_version"
  val DIM_MEDUSA_TERMINAL_USER = "dim_medusa_terminal_user"
  val DIM_MEDUSA_PROGRAM = "dim_medusa_program"
  val DIM_MEDUSA_PROGRAM_COLUMN = "program_sk,sid"
  val DIM_MEDUSA_TERMINAL_USER_COLUMN = "terminal_sk,user_id"


  /*------------------------------------------add by michael end------------------------------------------*/

  // path
  // 定义moretv的launcherArea集合
  val MoretvLauncherAreaNAVI = Array("search", "setting") //For moretv,navi包含了search和setting两种
  //For moretv, 将这些内容归属于分类信息中
  val MoretvLauncherCLASSIFICATION = Array("history", "movie", "tv", "live", "hot", "zongyi", "comic", "mv", "jilu", "xiqu", "sports",
    "kids_home", "subject")
  val MoretvLauncherUPPART = Array("watchhistory", "otherswatch", "hotrecommend", "TVlive")
  // 定义moretv的launcher的accessArea集合
  val MoretvLauncherAccessLocation = Array("search", "setting", "history", "movie", "tv", "live", "hot", "zongyi", "comic", "mv",
    "jilu", "xiqu", "sports", "kids_home", "subject")
  val MoretvPageInfo = Array("history", "movie", "tv", "zongyi", "hot", "comic", "mv", "xiqu", "sports", "jilu", "subject", "live",
    "search", "kids_home")
  val MoretvPageDetailInfo = Array("search", "hot_jiaodian", "1_hot_tag_xinwenredian", "hot_zhuanti",
    "1_hot_tag_chuangyidongzuo", "1_hot_tag_yinshiduanpian", "1_hot_tag_youxi", "danmuzhuanqu", "1_hot_tag_qingsonggaoxiao",
    "1_hot_tag_shenghuoshishang", "1_hot_tag_yulebagua", "1_hot_tag_vicezhuanqu", "1_hot_tag_yinyuewudao",
    "1_hot_tag_wuhuabamen", "history", "collect", "subjectcollect", "mytag", "tag", "reservation", "multi_search", "movie_hot",
    "movie_7days", "movie", "movie_jujiaodian", "movie_zhuanti", "movie_teseyingyuan", "movie_star", "movie_yugao",
    "movie_yiyuan", "movie_xilie", "movie_erzhan", "movie_aosika", "movie_comic", "movie_hollywood", "movie_huayu",
    "movie_yazhou", "movie_lengmen", "1_movie_tag_dongzuo", "1_movie_tag_kehuan", "movie_yueyu", "collect", "tv_genbo",
    "tv_zhuanti", "dianshiju_tuijain", "tv_kangzhanfengyun", "tv_meizhouyixing", "1_tv_area_xianggang", "1_tv_area_hanguo",
    "tv_julebu", "tv_xianxiaxuanhuan", "1_tv_area_neidi", "1_tv_area_oumei", "tv_changju", "1_tv_area_riben", "jilu_xingzhen",
    "1_tv_area_taiwan", "1_tv_area_yingguo", "1_tv_area_qita", "tv_yueyu", "tv", "p_zongyi_hot_1", "zongyi_weishi",
    "zongyi_zhuanti", "dalu_jingxuan", "hanguo_jingxuan", "oumei_jingxuan", "gangtai_jingxuan", "zongyi_shaoer", "xiangsheng_zongyi",
    "1_zongyi_tag_zhenrenxiu", "1_zongyi_tag_fangtan", "1_zongyi_tag_youxi", "1_zongyi_tag_gaoxiao", "1_zongyi_tag_gewu",
    "1_zongyi_tag_shenghuo", "1_zongyi_tag_quyi", "1_zongyi_tat_caijing", "1_zongyi_tag_fazhi", "1_zongyi_tag_bobao", "yulebagua_zongyi",
    "1_zongyi_tag_qita", "hot_comic_top", "comic_zhujue", "dongman_xinfan", "movie_comic", "comic_zhuanti", "comic_jingdian",
    "comic_guoyu", "comic_dashi", "comic_tags_jizhang", "1_comic_tags_rexue", "1_comic_tags_gaoxiao", "1_comic_tags_meishaonu",
    "1_comic_tags_qingchun", "1_comic_tags_lizhi", "1_comic_tags_huanxiang", "1_comic_tags_xuanyi", "1_comic_tag_qita",
    "p_document_1", "jishi_wangpai", "jishi_zhuanti", "jilu_vice", "1_jilu_station_bbc", "1_jilu_station_ngc",
    "1_jilu_station_nhk", "jilu_meishi", "1_jilu_tags_junshi", "1_jilu_tags_ziran", "1_jilu_tags_shehui", "1_jilu_tags_renwu",
    "1_jilu_tags_lishi", "1_jilu_tags_ted", "1_jilu_tags_keji", "1_jilu_tags_qita", "1_xiqu_tag_guangchangwu",
    "1_zongyi_tag_quyi", "1_tv_xiqu_tag_jingju", "1_tv_xiqu_tag_yuju", "1_tv_xiqu_tag_yueju", "1_tv_xiqu_tag_huangmeixi",
    "1_tv_xiqu_tag_errenzhuan", "1_tv_xiqu_tag_hebeibangzi", "1_tv_xiqu_tag_jinju", "1_tv_xiqu_tag_xiju",
    "1_tv_xiqu_tag_qingqiang", "1_tv_xiqu_tag_chaoju", "1_tv_xiqu_tag_pingju", "1_tv_xiqu_tag_huaguxi", "1_xiqu_tags_aoju",
    "1_xiqu_tag_gezaixi", "1_xiqu_tags_lvju", "1_tv_xiqu_tag_huju", "1_tv_xiqu_tag_huaiju", "1_tv_xiqu_tag_chuanju",
    "1_tv_xiqu_tag_wuju", "1_tv_xiqu_tag_kunqu", "1_tv_xiqu_tag_suzhoutanchang", "movie_zhuanti", "tv_zhuanti",
    "zongyi_zhuanti", "comic_zhuanti", "kid_zhuanti", "hot_zhuanti", "jilu_zhuanti", "movie_star", "movie_xilie",
    "tv_meizhouyixing", "zongyi_weishi", "comic_dashi", "tv_jingdianchongwen", "comic_guoman", "jilu_maoshu", "jilu_douban",
    "comic_mingzuo", "tv_shujia", "haoshengying_zongyi", "wangzhanzizhi_zongyi", "tv_yuansheng", "tv_guowai_lianzai")


  val MORETVCONTENTTYPE = Array("history", "movie", "tv", "zongyi", "hot", "comic", "mv", "xiqu", "sports", "jilu", "subject")
  val MORETVPAGETABINFOFOUR = Array("kids_home")
  val MORETVPAGETABINFOTHREE = Array("history", "movie", "tv", "zongyi", "hot", "comic", "mv", "xiqu", "jilu", "subject")
  val MORETVPATHSUBCATEGORY = Array("similar", "peoplealsolike", "guessyoulike")
  val MORETVPATHSPECIALCATEGORY = Array("tag", "subject", "star")


  val MedusaPathSubAccessPath = Array("similar", "peoplealsolike", "guessyoulike")
  val MedusaPathProperty = Array("subject", "tag", "star")

  // 定义medusa的launcherArea集合、launcherAccessLocation集合
  val MedusaLauncherArea = Array("classification", "my_tv", "live", "recommendation", "foundation", "navi")
  val MedusaLauncherAccessLocation = Array("history", "collect", "account", "movie", "tv", "zongyi", "jilu", "comic", "xiqu",
    "kids", "hot", "mv", "sport", "top_new", "top_hot", "top_star", "top_collect", "interest_location",
    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14")
  val MedusaLive = "live" // 用于处理直播的特殊情况
  // 定义medusa的page页面的类型
  val MedusaPageInfo = Array("rank", "everyone_watching", "history", "search", "accountcenter_home", "movie", "tv", "zongyi", "jilu",
    "comic", "xiqu", "kids_home", "hot", "mv", "sport_home")
  // 定义page页面中的详细信息
  val MedusaPageDetailInfo = Array(
    "收藏追看", "节目预约", "标签订阅", "专题收藏", "明星关注",
    "热播动漫", "动漫主角", "新番上档", "动漫电影", "动漫专题", "8090经典", "国语动画", "动画大师", "机甲战斗", "热血冒险", "轻松搞笑", "后宫萝莉",
    "青春浪漫", "励志治愈", "奇幻魔法", "悬疑推理", "其他分类", "国漫精选", "名作之壁",
    "everyone_nearby", "top_new", "top_hot", "top_star", "top_collect",
    "院线大片", "七日更新", "猜你喜欢", "电影聚焦点", "电影专题", "特色影院", "影人专区", "抢先预告", "亿元票房", "系列电影", "战争风云", "奥斯卡佳片",
    "动画电影", "好莱坞巨制", "华语精选", "日韩亚太", "冷门佳片", "犀利动作", "科学幻想", "粤语佳片",
    "华语热播", "电视剧专题", "卫视常青剧", "抗战风云", "剧星专区", "香港TVB", "韩剧热流", "10亿俱乐部", "仙侠玄幻", "大陆剧场", "特色美剧", "长剧欣赏",
    "日剧集锦", "台湾剧集", "英伦佳剧", "其他地区", "粤语专区", "猜你喜欢", "经典重温", "最强暑期档", "电视原声", "海外同步",
    "综艺热播", "卫视强档", "综艺专题", "大陆精选", "韩国精选", "欧美精选", "港台精选", "少儿综艺", "真人秀场", "情感访谈", "游戏竞技", "爆笑搞怪",
    "歌舞晚会", "时尚生活", "说唱艺术", "财经民生", "社会法制", "新闻播报", "其他分类", "2016新歌声", "最强笑点", "娱乐八卦",
    "纪实热播", "王牌栏目", "纪实专题", "VICE专区", "BBC", "国家地理", "NHK", "美食大赏", "军事风云", "自然万象", "社会百态", "人物大观",
    "历史钩沉", "公开课", "前沿科技", "其他分类", "豆瓣高分", "猫叔推荐", "刑侦档案", "相声小品",
    "我的收藏", "今日焦点", "新闻热点", "资讯专题", "创意运动", "影视短片", "游戏动画", "生活时尚", "娱乐八卦", "音乐舞蹈", "五花八门",
    "电台", "热门歌手", "正在流行", "MV首发", "精选集", "演唱会", "排行榜",
    "广场舞", "戏曲综艺", "京剧", "豫剧", "越剧", "黄梅戏", "二人转", "河北梆子", "晋剧", "锡剧", "秦腔", "潮剧",
    "评剧", "花鼓戏", "粤剧", "歌仔戏", "吕剧", "沪剧", "淮剧", "川剧", "婺剧", "昆曲", "苏州弹唱",
    "kids_collect*观看历史", "kids_collect*收藏追看", "kids_collect*专题收藏", "kids_anim*动画明星", "kids_anim*少儿热播",
    "kids_anim*动画专题", "kids_anim*少儿电影", "kids_anim*儿童综艺", "kids_anim*0-3岁", "kids_anim*最新出炉",
    "kids_anim*4-6岁", "kids_anim*7-10岁", "kids_anim*英文动画", "kids_anim*搞笑", "kids_anim*机战", "kids_anim*亲子", "kids_anim*探险",
    "kids_anim*中文动画", "kids_anim*亲子交流", "kids_anim*益智启蒙", "kids_anim*童话故事", "kids_anim*教育课堂", "kids_rhymes*随便听听",
    "kids_rhymes*儿歌明星", "kids_rhymes*儿歌热播", "kids_rhymes*儿歌专题", "kids_rhymes*英文儿歌", "kids_rhymes*舞蹈律动", "午夜场", "抢鲜预告", "重磅追剧", "二次元", "次元经典")

  //除了少儿，音乐，体育
  val MedusaPageDetailInfoFromSite = Array("系列电影", "苏州弹唱", "战争风云", "动画电影", "时尚生活", "越剧", "粤语佳片", "科学幻想", "七日更新", "评剧", "仙侠玄幻", "青春浪漫", "特色影院", "经典重温", "特色美剧", "二人转", "NHK", "奥斯卡佳片", "电视剧专题", "潮剧", "豆瓣高分", "锡剧", "娱乐八卦", "昆曲", "轻松搞笑", "国漫精选", "院线大片", "悬疑推理", "新闻热点", "午夜场", "五花八门", "吕剧", "黑马之作", "卫视常青剧", "短视频专题", "铁血军魂", "社会法制", "广场舞", "海外同步", "真人秀场", "热血冒险", "台湾剧集", "影人专区", "财经民生", "VICE专区", "电影聚焦点", "创意运动", "华语热播", "卫视强档", "说唱艺术", "相声小品", "剧星专区", "冷门佳片", "收藏", "犀利动作", "花鼓戏", "励志治愈", "生活时尚", "华语精选", "历史钩沉", "BBC", "英伦佳剧", "淮剧", "音乐舞蹈", "港台精选", "川剧", "豫剧", "综艺专题", "国语动画", "王牌栏目", "大陆精选", "美食大赏", "粤语专区", "动漫专题", "热播动漫", "爆笑搞怪", "后宫萝莉", "人物大观", "纪实专题", "10亿俱乐部", "抢鲜预告", "好莱坞巨制", "少儿综艺", "电视原声", "新闻专题", "次元经典", "影视短片", "大陆剧场", "秦腔", "国家地理", "其他地区", "沪剧", "王牌综艺", "公开课", "粤剧", "游戏动画", "动漫主角", "今日焦点", "自然万象", "歌舞晚会", "筛选", "前沿科技", "婺剧", "猜你喜欢", "综艺热播", "抗战风云", "弹幕专区", "社会百态", "亿元票房", "大话汽车", "其他分类", "情感访谈", "黄梅戏", "新番上档", "纪实热播", "河北梆子", "动画大师", "奇幻魔法", "电影专题", "香港TVB", "日剧集锦", "短视频栏目", "最强笑点", "新闻播报", "欧美精选", "京剧", "机甲战斗", "晋剧", "歌仔戏", "8090经典", "重磅追剧", "戏曲综艺", "游戏竞技", "我的收藏", "搜索", "刑侦档案", "二次元")

  /*------------------------------------------add by zhu.bingxin start------------------------------------------------*/

  //定义4X路径path中的变量名称
  val PAGE_TYPE = "page_type"
  val PAGE_URL = "page_url"
  val PAGE_TAB = "page_tab"
  val ACCESS_AREA = "access_area"
  val SUB_ACCESS_AREA = "sub_access_area"
  val LOCATION_INDEX = "location_index"
  val LINK_TYPE = "link_type"
  val LINK_VALUE = "link_value"
  val SUB_SUBJECT_CODE = "sub_subject_code"
  val FILTER_CONDITION = "filter_condition"
  val SEARCH_CONTENT = "search_content"
  val PAGE_ID = "page_id"


  //定义4Xpath中的page_type
  val LAUNCHERACTIVITY = "LauncherActivity" //首页
  val RECHOMEACTIVITY = "RecHomeActivity"
  //推荐位首页（包括体育和游戏）
  val KIDSHOMEACTIVITY = "KidsHomeActivity"
  //少儿首页
  val VODACTIVITY = "VodActivity"
  //通用频道页、专区页
  val KIDSANIMACTIVITY = "KidsAnimActivity" //少儿看动画&学知识
  val KIDSRHYMESACTIVITY = "KidsRhymesActivity"
  //少儿听儿歌
  val KIDSCOLLECTACTIVITY = "KidsCollectActivity"
  //猫猫屋首页
  val DETAILHOMEACTIVITY = "DetailHomeActivity"
  //详情页
  val SPORTDETAILACTIVITY = "SportDetailActivity"
  //体育详情页、游戏详情页
  val PLAYACTIVITY = "PlayActivity"
  //点播
  val CAROUSELACTIVITY = "CarouselActivity"
  //轮播
  val LIVECHANNELACTIVITY = "LiveChannelActivity"
  //网络直播列表页
  val SPORTMATCHACTIVITY = "SportMatchActivity"
  //赛事赛程
  val SUBJECTHOMEACTIVITY = "SubjectHomeActivity"
  //专题
  val GROUPSUBJECTACTIVITY = "GroupSubjectActivity"
  //小视频专题
  val SMALLVIDEOACTIVITY = "SmallVideoActivity"
  //组合专题
  val TAGSUBSCRIBEACTIVITY = "TagSubscribeActivity"
  //标签页
  val STARACTIVITY = "StarActivity"
  //明星页
  val USERCENTERACTIVITY = "UserCenterActivity"
  //用户中心
  val MEMBERCENTERACTIVITY = "MemberCenterActivity"
  //会员中心
  val FILTERACTIVITY = "FilterActivity"
  //筛选页
  val SEARCHACTIVITY = "SearchActivity"
  //搜索页
  val MESSAGECENTERACTIVITY = "MessageCenterActivity"
  //消息中心
  val OPENSCREENACTIVITY = "openScreenActivity" //开屏页

  val SPORTLIVEACTIVITY = "SportLiveActivity" //体育直播页

  val SUBJECTTRANSFERACTIVITY = "SubjectTransferActivity" // 专题中转页面

  //开通腾讯会员的点击入口值
  val MTVIP = "MTVIP"


  /*------------------------------------------add by zhu.bingxin end--------------------------------------------------*/
}
