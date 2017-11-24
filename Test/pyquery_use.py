#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/24 上午12:02
# @Author  : Hou Rong
# @Site    : 
# @File    : pyquery_use.py
# @Software: PyCharm
import pyquery

content = '''
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
<meta name="google-site-verification" content="DVVM1p1HEm8vE1wVOQ9UjcKP--pNAsg_pleTU5TkFaM" />
<meta name="renderer" content="webkit">
<meta http-equiv="X-UA-Compatible" content="IE=Edge,chrome=1">
<meta http-equiv="mobile-agent" content="format=html5; url=http://m.qyer.com/place/praslin-island/sight/" />
<meta http-equiv="Cache-Control" content="no-transform" />


<title>普拉兰岛旅游景点_普拉兰岛景点介绍_普拉兰岛著名旅游景点攻略 - 穷游网</title>
<meta name="keywords" content="普拉兰岛景点介绍,普拉兰岛景点推荐,普拉兰岛景点排行,普拉兰岛景点攻略" />
<meta name="description" content="普拉兰岛旅游攻略，普拉兰岛景点攻略，穷游网为旅行者提供普拉兰岛景点介绍、普拉兰岛景点推荐、普拉兰岛景点排行等信息，为穷游er制订普拉兰岛出游计划提供参考。" />


<link rel="shortcut icon" href="//www.qyer.com/favicon.ico" />

<script type='text/javascript'>
var PLACE = PLACE || {
	TYPE:'city',
	PID :'10071'
};
if (PLACE.TYPE == "poi"){
    PLACE.CITYID = "";
}
</script>
<script type="text/javascript">
window.__qRequire__ = {
    version: '10',
    combineCSS: []
};
</script>

<link rel="stylesheet" href="//common1.qyerstatic.com/place/css/common/common_css_dc0f2871da7f4ad2b027ba846632c983.css">


<script src="//common1.qyerstatic.com/place/js/common/common_js_08e40206994b88dcce9260a6453dafce.js"></script>
</head>

<body>










<script>
    window.QYER={uid:[0][0]||0};

    window._RATrack = window._RATrack||{
        'platform':'web',
        'channel':'place',
        'page_type':'list',
        'ugc_type':'place_list',
        'ugc_content':'10071'
    };
</script>

<link href="//static.qyer.com/models/common/component/headfoot/dist/headerfoot_black.min.css"  rel="stylesheet" />
<script src="//static.qyer.com/models/common/component/headfoot/dist/headerfoot_black.min.js" async="async"></script>




<div class="q-layer-header">
    <div class="header-inner">
        <a data-bn-ipg="head-logo" href="//www.qyer.com"><img class="logo" src="//static.qyer.com/models/common/component/headfoot/icon/logo_116x36.png" width="58" height="18" /></a>
        <div class="nav">
            <ul class="nav-ul">
                <li class="nav-list nav-list-selected"><a class="nav-span" href="//place.qyer.com/" data-bn-ipg="head-nav-place" title="穷游目的地">目的地</a></li>
                <li class="nav-list "><a class="nav-span" href="//guide.qyer.com/" data-bn-ipg="head-nav-guide" title="穷游锦囊">锦囊</a></li>
                <li class="nav-list nav-list-layer  ">
                    <a class="nav-span" href="//bbs.qyer.com/" data-bn-ipg="head-nav-community" title="穷游论坛">社区<i class="iconfont icon-jiantouxia"></i></a>
                    <div class="q-layer q-layer-nav q-layer-arrow">
                        <ul>
                            <li class="nav-list-layer">
                                <a href="//bbs.qyer.com" data-bn-ipg="head-nav-bbs" title="穷游论坛"><i class="iconfont icon-bbs1"></i> 旅行论坛 <i class="iconfont icon-jiantouyou"></i></a>
                                <div class="q-layer q-layer-section">
                                    <div class="q-layer">
                                        <div class="section-title">
                                            <a class="more" href="//bbs.qyer.com">全部版块<i class="iconfont icon-jiantouyou"></i></a>
                                            <span>热门版块</span>
                                        </div>


                                                                                  

                                    </div>
                                </div>
                            </li>
                            <li><a href="//ask.qyer.com/" data-bn-ipg="head-nav-ask" title="旅行问答"><i class="iconfont icon-ask1"></i> 旅行问答</a></li>
                            <li><a href="//jne.qyer.com/" data-bn-ipg="head-nav-qlab" title="JNE旅行生活美学" target="_blank"><i class="iconfont icon-jne1"></i> JNE旅行生活美学</a></li>
                            <li><a href="//bbs.qyer.com/forum-2-1.html" data-bn-ipg="head-nav-play" title="结伴同游" target="_blank"><i class="iconfont icon-play"></i> 结伴同游</a></li>
                            <li><a href="//rt.qyer.com/" data-bn-ipg="head-nav-rt" title="负责任的旅行" target="_blank"><i class="iconfont icon-rt1"></i> 负责任的旅行</a></li>
                            <li><a href="//zt.qyer.com/" data-bn-ipg="head-nav-zt" title="特别策划" target="_blank"><i class="iconfont icon-zt"></i> 特别策划</a></li>
                        </ul>
                    </div>
                </li>
                <li class="nav-list nav-list-plan ">
                    <a class="nav-span" href="//plan.qyer.com/" data-bn-ipg="head-nav-plan" title="穷游行程助手">行程助手</a>
                </li>
                <li class="nav-list nav-list-layer nav-list-zuishijie ">
                    <a class="nav-span" href="//z.qyer.com/" data-bn-ipg="head-nav-lastminute" title="商城">商城<i class="iconfont icon-jiantouxia"></i></a>
                    <div class="gif">
                        <img class="gif1" src="//static.qyer.com/models/common/component/headfoot/icon/gif.gif" height="19" width="44" >
                    </div>
                    <div class="q-layer q-layer-nav q-layer-arrow">
                        <ul>
                            <li><a href="//z.qyer.com/package/" data-bn-ipg="head-nav-package" target="_blank" title="机酒自由行"><i class="iconfont icon-package"></i> 机酒自由行</a></li>
                            <li><a href="//z.qyer.com/local/" data-bn-ipg="head-nav-local" target="_blank" title="当地玩乐"><i class="iconfont icon-local"></i> 当地玩乐</a></li>
                            <li><a href="//z.qyer.com/visa/" data-bn-ipg="head-nav-visa" target="_blank" title="签证"><i class="iconfont icon-visa1"></i> 签证</a></li>
                            <li><a href="//z.qyer.com/car/?utm_source=c03729731-pc&utm_medium=partner&utm_campaign=entry&utm_term=qyer_nav" data-bn-ipg="head-nav-car" target="_blank" title="租车自驾"><i class="iconfont icon-car"></i> 租车自驾</a></li>
                            <li><a href="//z.qyer.com/cruise/" data-bn-ipg="head-nav-cruise" target="_blank" title="邮轮"><i class="iconfont icon-cruise"></i> 邮轮</a></li>
                            <li><a href="//bx.qyer.com/" data-bn-ipg="head-nav-insure" target="_blank" title="保险"><i class="iconfont icon-bx"></i> 保险</a></li>
                            <li><a href="//z.qyer.com/travelgroup" data-bn-ipg="head-nav-travelgroup" target="_blank" title="私人订制"><i class="iconfont icon-travelgroup"></i> 私人定制</a></li>
                        </ul>
                    </div>
                </li>
                <li class="nav-list "><a class="nav-span" href="//flight.qyer.com/" data-bn-ipg="head-nav-plane" title="机票">机票</a></li>
                <li class="nav-list nav-list-layer ">
                    <a class="nav-span" href="//hotel.qyer.com/" data-bn-ipg="head-nav-hotel" title="穷游酒店">酒店&middot;民宿 <i class="iconfont icon-jiantouxia"></i></a>
                    <div class="q-layer q-layer-nav q-layer-arrow q-layer-arrow1">
                        <ul>
                            <li><a href="//hotel.qyer.com" data-bn-ipg="head-nav-booking" title="酒店"><i class="iconfont icon-hotel1"></i> 酒店</a></li>
                            <li><a href="//www.qyer.com/goto.php?url=https%3A%2F%2Fwww.airbnbchina.cn%2F%3Faf%3D104561116%26c%3DRESERVATION" data-bn-ipg="head-nav-airbnb" title="爱彼迎" target="_blank"><i class="iconfont icon-airbnb1"></i> 爱彼迎</a></li>
                            <li><a href="//www.qyer.com/shop/" data-bn-ipg="head-nav-shop" title="华人旅馆"><i class="iconfont icon-shop"></i> 华人旅馆</a></li>
                        </ul>
                    </div>
                </li>
                <li class="nav-list nav-list-layer ">
                    <a class="nav-span" href="//z.qyer.com/explore/" data-bn-ipg="head-nav-explore" title="独家深度">独家深度 <i class="iconfont icon-jiantouxia"></i></a>
                    <div class="q-layer q-layer-nav q-layer-arrow q-layer-arrow1">
                        <ul>
                            <li><a href="//z.qyer.com/zt/leadertour/" data-bn-ipg="head-nav-leadertour" title="特色长线"><i class="iconfont icon-leadertour"></i> 特色长线</a></li>
                            <li><a href="//z.qyer.com/citywalk/" data-bn-ipg="head-nav-citywalk" title="独家日游"><i class="iconfont icon-citywalk"></i> 独家日游</a></li>
                            <li><a href="//z.qyer.com/explore/#qHome" data-bn-ipg="head-nav-qhome" title="Q-Home"><i class="iconfont icon-qhome"></i> Q-Home</a></li>
                        </ul>
                    </div>
                </li>

                <li class="nav-list nav-list-layer">
                    <a class="nav-span" href="//app.qyer.com/guide/" data-bn-ipg="head-nav-app" title="穷游App">穷游App <i class="iconfont icon-jiantouxia"></i></a>
                    <div class="q-layer q-layer-nav q-layer-arrow">
                        <ul>
                            <li><a href="//app.qyer.com/guide/" data-bn-ipg="head-nav-app" title="穷游App"><i class="iconfont icon-qyer"></i> 穷游App</a></li>
                            <li><a href="//app.qyer.com/plan/" data-bn-ipg="head-nav-plan" title="行程助手App"><i class="iconfont icon-planapp"></i> 行程助手App</a></li>
                            <li><a href="//guide.qyer.com/app/" data-bn-ipg="head-nav-guide" title="穷游锦囊App"><i class="iconfont icon-guide"></i> 穷游锦囊App</a></li>
                        </ul>
                    </div>
                </li>
            </ul>
        </div>
        <div class="fun">
            <div id="siteSearch" class="nav-search">
                <form action="//search.qyer.com/index" method="get">
                    <input class="txt" name="wd" type="text" autocomplete="off">
                    <button class="btn" type="submit"><i class="iconfont icon-sousuo"></i><span>搜索</span></button>
                </form>
            </div>
            <div id="js_qyer_header_userStatus" class="status">
              <div class="login show">
                    <a class="otherlogin-link" href="javascript:;" url="http://place.qyer.com/praslin-island/sight/" rel="noflow" data-bn-ipg="index-head-un-qq" data-type="qq"><i class="iconfont icon-qq"></i></a>
                    <a class="otherlogin-link" href="javascript:;" url="http://place.qyer.com/praslin-island/sight/" rel="noflow" data-bn-ipg="index-head-un-weibo" data-type="weibo"><i class="iconfont icon-weibo"></i></a>
                    <a class="otherlogin-link" href="javascript:;" url="http://place.qyer.com/praslin-island/sight/" rel="noflow" data-bn-ipg="index-head-un-wechat" data-type="weixin"><i class="iconfont icon-weixin"></i></a>

                    <a href="https://passport.qyer.com/register/mobile?ref=http%3A%2F%2Fplace.qyer.com%2Fpraslin-island%2Fsight%2F" data-bn-ipg="index-head-un-register">注册</a>
                    <a href="https://passport.qyer.com/login?ref=http%3A%2F%2Fplace.qyer.com%2Fpraslin-island%2Fsight%2F" data-bn-ipg="index-head-un-login">登录</a>
              </div>
            </div>
        </div>
    </div>
</div>

<!--token:d41d8cd98f00b204e9800998ecf8427e-->
<!-- poi详情导航, 在frame中引入此layout文件, 需要判断是poi页面, 则引入css, 不是poi页面, 不引入css -->


 

<div class="qyer_head_crumbg">
	<div class="qyer_head_crumb">
        <!-- 目的地面包屑 -->

<span class="text drop">
        <a href="//place.qyer.com" data-bn-ipg="3-2">目的地</a>
        <em class="arrow" data="crumb_country"></em>
</span>
    <span class='space'>&gt;</span><span class='text drop'><a href='http://place.qyer.com/seychelles/' data-bn-ipg='3-4'>塞舌尔</a><em class='arrow' data='crumb_city'></em></span><span class='space'>&gt;</span><span class='text drop'><a href='//place.qyer.com/praslin-island/'>普拉兰岛</a><em class='arrow' data='crumb_cate'></em></span><span class='space'>&gt;</span><h1 class='current'>普拉兰岛景点攻略</h1>

<div class="qyer_head_crumb_pulldown crumb_country" >

            
                    <ul>
                <li><a href="http://place.qyer.com/egypt/" data-bn-ipg="3-3-2">埃及 Egypt</a></li>
        
        
                    
                <li><a href="http://place.qyer.com/morocco/" data-bn-ipg="3-3-2">摩洛哥 Morocco</a></li>
        
        
                    
                <li><a href="http://place.qyer.com/south-africa/" data-bn-ipg="3-3-2">南非 South Africa</a></li>
        
        
                    
                <li><a href="http://place.qyer.com/kenya/" data-bn-ipg="3-3-2">肯尼亚 Kenya</a></li>
        
        
                    
                <li><a href="http://place.qyer.com/mauritius/" data-bn-ipg="3-3-2">毛里求斯 Mauritius</a></li>
        
        
                    
                <li><a href="http://place.qyer.com/tanzania/" data-bn-ipg="3-3-2">坦桑尼亚 Tanzania</a></li>
        
        
                    
                <li><a href="http://place.qyer.com/ethiopia/" data-bn-ipg="3-3-2">埃塞俄比亚 Ethiopia</a></li>
        
        
                    
                <li><a href="http://place.qyer.com/seychelles/" data-bn-ipg="3-3-2">塞舌尔 Seychelles</a></li>
        
        
                    
                <li><a href="http://place.qyer.com/tunisia/" data-bn-ipg="3-3-2">突尼斯 Tunisia</a></li>
        
        
                    
                <li><a href="http://place.qyer.com/zambia/" data-bn-ipg="3-3-2">赞比亚 Zambia</a></li>
                    </ul>
        
                    <ul>
        
                    
                <li><a href="http://place.qyer.com/algeria/" data-bn-ipg="3-3-2">阿尔及利亚 Algeria</a></li>
        
        
                    
                <li><a href="http://place.qyer.com/namibia/" data-bn-ipg="3-3-2">纳米比亚 Namibia</a></li>
        
        
                    
                <li><a href="http://place.qyer.com/zimbabwe/" data-bn-ipg="3-3-2">津巴布韦 Zimbabwe</a></li>
        
        
                    
                <li><a href="http://place.qyer.com/uganda/" data-bn-ipg="3-3-2">乌干达 Uganda</a></li>
        
        
                    
                <li><a href="http://place.qyer.com/nigeria/" data-bn-ipg="3-3-2">尼日利亚 Nigeria</a></li>
        
        
                    
                <li><a href="http://place.qyer.com/madagascar/" data-bn-ipg="3-3-2">马达加斯加 Madagascar</a></li>
        
        
                    
                <li><a href="http://place.qyer.com/ghana/" data-bn-ipg="3-3-2">加纳 Ghana</a></li>
        
        
                    
                <li><a href="http://place.qyer.com/sudan/" data-bn-ipg="3-3-2">苏丹 Sudan</a></li>
        
        
                    
                <li><a href="http://place.qyer.com/lesotho/" data-bn-ipg="3-3-2">莱索托 Lesotho</a></li>
        
        
                    
                <li><a href="http://place.qyer.com/angola/" data-bn-ipg="3-3-2">安哥拉 Angola</a></li>
                    </ul>
        
        
            	<div class="cb"></div>
            <p class="more"><a href="//place.qyer.com/africa/" data-bn-ipg="3-3-2">更多国家&gt;&gt;</a></p>
    </div>







<div class="qyer_head_crumb_pulldown crumb_cate" style="width: 160px;">

            
                    <ul>
                
        <li>
            <a href="//place.qyer.com/praslin-island/sight/">
                普拉兰岛景点
            </a>
        </li>
        
        
        
                    
                
        <li>
            <a href="//place.qyer.com/praslin-island/activity/">
                普拉兰岛体验
            </a>
        </li>
        
        
        
                    
                
        <li>
            <a href="//place.qyer.com/praslin-island/transportation/">
                普拉兰岛交通
            </a>
        </li>
        
        
        
                    
                
        <li>
            <a href="//hotel.qyer.com/praslin-island/">
                普拉兰岛住宿
            </a>
        </li>
        
        
        
                    
                
        <li>
            <a href="//place.qyer.com/praslin-island/shopping/">
                普拉兰岛购物
            </a>
        </li>
        
        
        
                    
                
        <li>
            <a href="//place.qyer.com/praslin-island/food/">
                普拉兰岛美食
            </a>
        </li>
        
        
        
                    </ul>
            	<div class="cb"></div>

</div>


    </div>
</div>

    <!-- 头部模块 before -->
<div class="plcTopBar clearfix">
    <!-- 国家 区域 城市 头部信息 公共 -->

<div class="plcTopBarL">
    <p class="plcTopBarNameEn">
        <a href="//place.qyer.com/praslin-island/" data-bn-ipg="place-city-top-entitle">
            Praslin Island
        </a>
    </p>
    <div class="plcTopBarNameCns clearfix">
        <p class="plcTopBarNameCn fontYaHei">
            <a href="//place.qyer.com/praslin-island/" data-bn-ipg="place-city-top-cntitle">
                普拉兰岛
            </a>
        </p>
                
        <!-- 台北 京都 曼谷 首尔 城市首页活动 -->
            </div>
</div>
    <!-- 国家 区域 城市 头部 天气 去过数 点评数 公共 -->

<div class="plcTopBarR">
    <div class="plcTopBarWeather">
                <a  class="weatherLink" href="weather/">
            <span class="cityName">天气</span>
            <img src="//static.qyer.com/images/place5/weather/weatherIcon-g-01.png" width="28" height="26" alt="" class="icon" />
            晴　25℃~30℃
        </a>
            </div>

    <div class="plcTopBarStat fontYaHei">
                <em>617</em>人去过这里, 
        <em>89</em>条目的地点评
            </div>
</div>
</div>
<!-- 头部模块 end -->

<!-- 头部菜单栏 before -->
<div class="plcMenuBarHolder">
<div class="plcMenuBars">
    <div class="plcMenuBar ">
        <ul class="plcMenuBarList fontYaHei">
                                    <li class="guide" id="menuBarGuide"><a href="//place.qyer.com/praslin-island/profile/" data-bn-ipg="place-area-nav-profile"><em>区域指南</em></a></li>
                                    <li class="poi" id="menuBarPoi"><a href="//place.qyer.com/praslin-island/alltravel/"><em>旅行地</em></a></li>
            <li class="route"><a href="//plan.qyer.com/search.php?tags=%E6%99%AE%E6%8B%89%E5%85%B0%E5%B2%9B" data-bn-ipg="place-area-nav-route"><em>行程路线</em></a></li>
                        <li class="travel"><a href="//place.qyer.com/praslin-island/travel-notes/" data-bn-ipg="place-area-nav-travelnote"><em>游记攻略</em></a></li>
            <li class="map"><a href="//place.qyer.com/praslin-island/map/" data-bn-ipg="place-area-nav-map"><em>地图</em></a></li>
        </ul>
                <p class="plcMenuBarAddPlan fontYaHei"><a href="javascript:void(0);" data-userid="0" data-type="area" data-pid="10071" data-addtoplan="true">加入行程</a></p>
                <div class="plcMenuBarLay guideDrop" id="menuBarGuideDrop" style="display: none;">
            <p class="layTitle fontYaHei">
                <a href="//place.qyer.com/praslin-island/profile/" data-bn-ipg="place-area-nav-profile"><em>区域指南</em></a>
            </p>
            <div class="layContent">
                <ul class="guideList">
                                                    <li class="list_overview">
                        <em class="tit"><a href="//place.qyer.com/praslin-island/profile/" data-bn-ipg="place-area-nav-目的地概况">目的地概况</a></em>
                                                <p class="txt">
                                                                                    <a href="//place.qyer.com/praslin-island/profile/#cate_167918" data-bn-ipg="place-area-nav-目的地速写">目的地速写</a>
                                 
                            <span>｜</span>
                                                                                                                                            <a href="//place.qyer.com/praslin-island/profile/#cate_167919" data-bn-ipg="place-area-nav-当地人生活">当地人生活</a>
                                 
                            <span>｜</span>
                                                                                                                                                                                                                                                            <a href="//place.qyer.com/praslin-island/profile/#cate_167922" data-bn-ipg="place-area-nav-文化">文化</a>
                                 
                            <span>｜</span>
                                                                                                                                            <a href="//place.qyer.com/praslin-island/profile/#cate_167923" data-bn-ipg="place-area-nav-环境">环境</a>
                                 
                            <span>｜</span>
                                                                                                                                                                                                                                                            <a href="//place.qyer.com/praslin-island/profile/#cate_167926" data-bn-ipg="place-area-nav-语言帮助">语言帮助</a>
                                 
                            <span>｜</span>
                                                                                                                                                                    </p>
                                            </li>
                                    <li class="list_domestic_traffic">
                        <em class="tit"><a href="//place.qyer.com/praslin-island/citytransport/" data-bn-ipg="place-area-nav-市内交通">市内交通</a></em>
                                                <p class="txt">
                                                                                                                                            <a href="//place.qyer.com/praslin-island/citytransport/#cate_167953" data-bn-ipg="place-area-nav-公交">公交</a>
                                 
                            <span>｜</span>
                                                                                                                                                                                                                                                                                                                                                                                                                                                            </p>
                                            </li>
                                    <li class="list_season">
                        <em class="tit"><a href="//place.qyer.com/praslin-island/when-to-go/" data-bn-ipg="place-area-nav-旅行日历">旅行日历</a></em>
                                                <p class="txt">
                                                                                    <a href="//place.qyer.com/praslin-island/when-to-go/#cate_167929" data-bn-ipg="place-area-nav-旅行季节">旅行季节</a>
                                                                                                            </p>
                                            </li>
                                    <li class="list_currency">
                        <em class="tit"><a href="//place.qyer.com/praslin-island/cost/" data-bn-ipg="place-area-nav-货币与花费">货币与花费</a></em>
                                                <p class="txt">
                                                                                    <a href="//place.qyer.com/praslin-island/cost/#cate_167931" data-bn-ipg="place-area-nav-消费水平">消费水平</a>
                                 
                            <span>｜</span>
                                                                                                                                            <a href="//place.qyer.com/praslin-island/cost/#cate_167932" data-bn-ipg="place-area-nav-货币兑换">货币兑换</a>
                                 
                            <span>｜</span>
                                                                                                                                            <a href="//place.qyer.com/praslin-island/cost/#cate_167933" data-bn-ipg="place-area-nav-银联">银联</a>
                                 
                            <span>｜</span>
                                                                                                                                                                                                    <a href="//place.qyer.com/praslin-island/cost/#cate_167935" data-bn-ipg="place-area-nav-小费">小费</a>
                                 
                            <span>｜</span>
                                                                                                                                                                                                                            </p>
                                            </li>
                                    <li class="list_information">
                        <em class="tit"><a href="//place.qyer.com/praslin-island/information/" data-bn-ipg="place-area-nav-实用信息">实用信息</a></em>
                                                <p class="txt">
                                                                                                                                                                                                    <a href="//place.qyer.com/praslin-island/information/#cate_167941" data-bn-ipg="place-area-nav-网络">网络</a>
                                 
                            <span>｜</span>
                                                                                                                                                                                                    <a href="//place.qyer.com/praslin-island/information/#cate_167943" data-bn-ipg="place-area-nav-邮局">邮局</a>
                                 
                            <span>｜</span>
                                                                                                                                                                    </p>
                                            </li>
                                    <li class="list_notice">
                        <em class="tit"><a href="//place.qyer.com/praslin-island/advice/" data-bn-ipg="place-area-nav-旅行须知">旅行须知</a></em>
                                                <p class="txt">
                                                                                    <a href="//place.qyer.com/praslin-island/advice/#cate_167911" data-bn-ipg="place-area-nav-穷游er忠告">穷游er忠告</a>
                                 
                            <span>｜</span>
                                                                                                                                            <a href="//place.qyer.com/praslin-island/advice/#cate_167912" data-bn-ipg="place-area-nav-负责任的旅行">负责任的旅行</a>
                                 
                            <span>｜</span>
                                                                                                                                            <a href="//place.qyer.com/praslin-island/advice/#cate_167913" data-bn-ipg="place-area-nav-安全">安全</a>
                                 
                            <span>｜</span>
                                                                                                                                                                                                                                                                                    </p>
                                            </li>
                                                </ul>
                                <p class="guideGap"></p>
                                                <div class="guideAdd">
                    <p class="tit">还有一些指南信息，等待你来补充~</p>
                    <p class="tags">
                                                <a class="tag_btn" href="//place.qyer.com/praslin-island/transportation/" data-bn-ipg="place-area-nav-contribute-城际交通" entry-id="167945"><i class="nav_icon nav_icon_traffic"></i>城际交通</a>
                                            </p>
                </div>
                            </div>
        </div>
        <div class="plcMenuBarLay poiDrop" id="menuBarPoiDrop" style="display: none;">
            <p class="layTitle fontYaHei"><a href="//place.qyer.com/praslin-island/alltravel/"><em>旅行地</em></a></p>
            <div class="layContent">
                <ul class="poiList">
                    <li class="sight">
                        <a href="//place.qyer.com/praslin-island/sight/" data-bn-ipg="place-area-nav-sight">
                            景点<span>(15)</span>
                        </a>
                    </li>
                    <li class="food">
                        <a href="//place.qyer.com/praslin-island/food/" data-bn-ipg="place-area-nav-food">
                            美食<span>(4)</span>
                        </a>
                    </li>
                    <li class="shopping"><a href="//place.qyer.com/praslin-island/shopping/" data-bn-ipg="place-area-nav-shopping">购物<span>(0)</span></a></li>
                    <li class="activity"><a href="//place.qyer.com/praslin-island/activity/" data-bn-ipg="place-area-nav-activity">活动<span>(2)</span></a></li>
                    <li class="mguide"><a href="//place.qyer.com/praslin-island/mguide/" data-bn-ipg="place-area-nav-mguide">微锦囊<span>(1)</span></a></li>
                </ul>
            </div>
        </div>
                        <div class="plcMenuBarLay cityDrop" id="menuBarCityDrop" style="display: none;">
            <p class="layTitle fontYaHei"><a href="//place.qyer.com/praslin-island/citylist-0-0-1/"><em>热门城市</em></a></p>
            <div class="layContent">
                                <p class="emptyContent" style="display: none;">这个国家暂时没有热门城市</p>
                            </div>
        </div>
            </div>
</div>
</div>
<script type="text/javascript">
    // 导航投稿编辑
    $('#menuBarGuideDrop').on('click', '.tag_btn', function (){
        if (! qyerUtil.isLogin()){
            qyerUtil.doLogin();
        } else {
            var entry_id = this.getAttribute('entry-id');
            var entry_children_id = this.getAttribute('editor-children-id');
            requirejs(['project/js/entryEditor'], function(editor){
                editor.open(entry_id, entry_children_id);
            });
        }
        return false;
    });
</script>
<!-- 头部菜单栏 end -->

<link rel="stylesheet" href="//common1.qyerstatic.com/place/css/common/poiList_css_706c8546d45dcbc409ff37333f5af1ce.css">

<script src="//common1.qyerstatic.com/place/js/common/poiList_js_07a8454c61dfdec03e4f3aaf52c365aa.js"></script>


<div class="plcListMgContainer">
    <div class="qyWrap">
        <div class="plcListMgTitles">
            <h2 class="title fontYaHei">普拉兰岛景点微锦囊</h2>
                    </div>
                <div class="plcListMgContent">
                        <div class="lists" >
                <ul class="list">
                                                                                                                                                                                                                                                                                                    <li class='item1'>
                        <a href="//place.qyer.com/mguide/2699" data-bn-ipg='place-poilist-mguide-1' target="_blank" title="塞舌尔三岛的那些顶级海滩">
                            <p class="photo"><img src="http://pic.qyer.com/album/user/990/3/SEBVQhkEZQ/index/500x300" width="478" height="auto" alt="塞舌尔三岛的那些顶级海滩" /></p>
                            <p class="photoMask"></p>
                            <div class="content">
                                <p class="face"><img src="" width="60" height="60" alt="hudiealai"/></p>
                                <h3 class="title fontYaHei">塞舌尔三岛的那些顶级海滩</h3>
                                <blockquote class="text">严格来说塞舌尔是一个国家。鉴于单独一岛面积十分袖珍，把三个主要岛屿合成一体来写。对于见惯了天朝泼墨写意、挥洒自如的名山大川的我们而言，塞舌尔更像是一处盆景。虽然面积不大，却是移步换景，别有一番情趣。尤其是为数众多的顶级海滩，吸引了世界各国的游客流连忘返。只是这盆景并非人工雕琢，多了一份浑然天成的质朴与纯真，让人对大自然的鬼斧神工肃然起敬。</blockquote>
                                <p class="tags">
                                                                    风景
                                                                    海滩
                                                                </p>
                            </div>
                            <div class="bottom"></div>
                        </a>
                    </li>
                                    </ul>
            </div>
                    </div>
            </div>
</div>



<div class="plcContainer">
    <div class="qyWrap">

        <div class="qyMain fl">
            
            <!-- 游记攻略 before -->
            <div class="plcPoilistModules" id="plcTravelLists">
                <h3 class="subTitle fontYaHei">普拉兰岛景点</h3>
                <ul class="plcPoiSort">
                    <li>
                        <p class="tit">旅行地：</p>
                        <p class="labels" id="poiSort">
                            <a href="//place.qyer.com/praslin-island/alltravel/"  data-id="0" data-bn-ipg="place-poilist-filter-classify-all">全部(27)</a>
                            <a href="//place.qyer.com/praslin-island/sight/" data-id="32" class="current" data-bn-ipg="place-poilist-filter-classify-sight">景点(15)</a>
                            <a href="//place.qyer.com/praslin-island/food/" data-id="78"  data-bn-ipg="place-poilist-filter-classify-food">美食(4)</a>
                            <a href="//place.qyer.com/praslin-island/shopping/" data-id="147"  data-bn-ipg="place-poilist-filter-classify-shopping">购物(0)</a>
                            <a href="//place.qyer.com/praslin-island/activity/" data-id="148"  data-bn-ipg="place-poilist-filter-classify-activity">活动(2)</a>
                        </p>
                    </li>
                                                                    <li id="poiSubsort">
                            <p class="tit">景点分类：</p>
                            <p class="moreBtn" id="poiSortMore">更多</p>
                            <p class="labels" id="poiSortLabels">
                                <a href="/praslin-island/sight/" class="current" data-id="0">全部</a>
                                                                <a href="/praslin-island/sight/tag204/" data-id="204" data-bn-ipg="place-poilist-filter-category" >海滩</a>
                                                                <a href="/praslin-island/sight/tag199/" data-id="199" data-bn-ipg="place-poilist-filter-category" >自然风光</a>
                                                                <a href="/praslin-island/sight/tag1326/" data-id="1326" data-bn-ipg="place-poilist-filter-category" >国家公园</a>
                                                                <a href="/praslin-island/sight/tag210/" data-id="210" data-bn-ipg="place-poilist-filter-category" >岛屿</a>
                                                                <a href="/praslin-island/sight/tag2014/" data-id="2014" data-bn-ipg="place-poilist-filter-category" >观景台</a>
                                       
                            </p>
                        </li>
                                                            </ul>
                <div class="plcPoiSift">
                    <p class="shaix">
                        <label><input type="radio" name="poi" value="1"><span>微锦囊推荐</span></label>
                        <label><input type="radio" name="poi"  value="2"><span data-bn-ipg='place-poilist-filter-lm'>有折扣</span></label>
                    </p>
                    <div id="s2" class="qui-select" data-ui-width="80px" data-ui-height="44px" data-value="0">
                        <strong class="titles">
                            <span class="title-text">按去过数</span>
                        </strong>
                        <div class="contents" style='display:none;' >
                            <ul>
                                                                <li data-value="0"><a href="javascript:void(0);" data-bn-ipg="place-poilist-rank-hot">按去过数</a></li>
                                <li data-value="1"><a href="javascript:void(0);" data-bn-ipg="place-poilist-rank-score">按评分</a></li>
                                <li data-value="2"><a href="javascript:void(0);" data-bn-ipg="place-poilist-rank-scorenum">按点评数</a></li>
                            </ul>
                        </div>
                    </div>
                </div>
                <div class="plcPoiLoading" id="poiLoading" style="display:none;"></div>
                
                                <div class="plcPoiEmpty" id="poiEmpty" style="display:none;">
                    <p style="display:none;">
                    这里暂时没有任何
                                            景点
                                        被微锦囊添加过
                    </p>
                    <p style="display:none;">
                        暂时没有
                                                    景点
                                                配有折扣
                        <br />
                        <a target='_blank' href='//z.qyer.com'>查看更多穷游折扣</a>
                    </p>
                </div>
                
                <ul class="plcPoiList" id="poiLists">
                                        <li class="clearfix">
						<div class="cntBox clearfix">
							<p class="pics">
								<a href="//place.qyer.com/poi/V2YJY1FmBz5TZ1I3/" target='_blank'>
									<img src="http://pic1.qyer.com/album/user/2141/92/Q0hRQxMFYUE/index/225x150" width="227" height="150">
                                    								</a>
							</p>
                            
							<h3 class="title fontYaHei">
								<a href="//place.qyer.com/poi/V2YJY1FmBz5TZ1I3/" target='_blank'>
                                                                            拉齐奥海滩
                                                                        &nbsp;&nbsp;
                                    <span>Anse Lazio</span>
                                </a>
							</h3>
                            
							<div class="info">
                                								<p class="stars">
                                    <span class='star'></span><span class='star'></span><span class='star'></span><span class='star'></span><span class='half'></span>
								</p>
								<span class="grade">9.6</span>
								<span class="dping">
									<a href="//place.qyer.com/poi/V2YJY1FmBz5TZ1I3/review/" target='_blank'>
                                        26人点评
                                    </a>
								</span>
                                                                
                                                                
                                                                <span class="infoSide">
                                                                    普拉兰岛
                                                                景点观光综合排名 <em class="rank orange">第1位</em>
                                </span>
                                							</div>
                            
							<div class="comment clearfix">
                                								<p class="user">
                                    <a href="//www.qyer.com/u/58843" target='_blank'>
                                        <img src="http://pic4.qyer.com/avatar/000/05/88/43/120?v=" width="32" height="32">
                                    </a>
                                </p>
                                <div class="txt">
                                    名气很大，也很美。公共交通到达以后还要翻山越岭10分钟。 两家很简陋的咖啡，还不给wifi用

                                </div>
								                                
                                                                <p class="mguideNumber">
                                    <a href="javascript:void(0)" data-id='200838' data-bn-ipg='poilist-poi-mguide-1'>
                                        1个微锦囊推荐了这里
                                    </a>
                                </p>
                                							</div>
                            
                            
                            <p data-ipg-add='place-poilist-wishto-add' data-ipg-delete='place-poilist-wishto-delete' class="plcAddToLikeList _addToLikeList" data-status="1" data-pid="200838" data-type="2" data-userid="0"></p>
                            <p data-bn-ipg='place-poilist-poi-addtoplan-1' class="goToMap" title="查看地图" data-pid="200838" data-type="poi"></p>
						</div>
					</li>
                                        <li class="clearfix">
						<div class="cntBox clearfix">
							<p class="pics">
								<a href="//place.qyer.com/poi/V2UJY1FnBzZTZFI5/" target='_blank'>
									<img src="http://pic.qyer.com/album/user/1660/85/QE9TQhICaU8/index/225x150" width="227" height="150">
                                    									<span class="label">用户热推</span>
                                    								</a>
							</p>
                            
							<h3 class="title fontYaHei">
								<a href="//place.qyer.com/poi/V2UJY1FnBzZTZFI5/" target='_blank'>
                                                                            五月谷
                                                                        &nbsp;&nbsp;
                                    <span>Vallee de Mai</span>
                                </a>
							</h3>
                            
							<div class="info">
                                								<p class="stars">
                                    <span class='star'></span><span class='star'></span><span class='star'></span><span class='star'></span><span class='null'></span>
								</p>
								<span class="grade">8.5</span>
								<span class="dping">
									<a href="//place.qyer.com/poi/V2UJY1FnBzZTZFI5/review/" target='_blank'>
                                        24人点评
                                    </a>
								</span>
                                                                
                                                                
                                                                <span class="infoSide">
                                                                    普拉兰岛
                                                                景点观光综合排名 <em class="rank orange">第2位</em>
                                </span>
                                							</div>
                            
							<div class="comment clearfix">
                                								<p class="user">
                                    <a href="//www.qyer.com/u/1313937" target='_blank'>
                                        <img src="" width="32" height="32">
                                    </a>
                                </p>
                                <div class="txt">
                                    虽然很小，但是我也没去过更大的热带森林了……里面挺好玩的，海椰子有特点。

                                </div>
								                                
                                							</div>
                            
                            
                            <p data-ipg-add='place-poilist-wishto-add' data-ipg-delete='place-poilist-wishto-delete' class="plcAddToLikeList _addToLikeList" data-status="1" data-pid="101006" data-type="2" data-userid="0"></p>
                            <p data-bn-ipg='place-poilist-poi-addtoplan-1' class="goToMap" title="查看地图" data-pid="101006" data-type="poi"></p>
						</div>
					</li>
                                        <li class="clearfix">
						<div class="cntBox clearfix">
							<p class="pics">
								<a href="//place.qyer.com/poi/V2UJYVFlBzJTZFI3/" target='_blank'>
									<img src="http://pic.qyer.com/album/user/1662/16/QE9TQBsBY0s/index/225x150" width="227" height="150">
                                    								</a>
							</p>
                            
							<h3 class="title fontYaHei">
								<a href="//place.qyer.com/poi/V2UJYVFlBzJTZFI3/" target='_blank'>
                                                                            科多尔海滩
                                                                        &nbsp;&nbsp;
                                    <span>Cote d’or Beach</span>
                                </a>
							</h3>
                            
							<div class="info">
                                								<p class="stars">
                                    <span class='star'></span><span class='star'></span><span class='star'></span><span class='star'></span><span class='half'></span>
								</p>
								<span class="grade">9.2</span>
								<span class="dping">
									<a href="//place.qyer.com/poi/V2UJYVFlBzJTZFI3/review/" target='_blank'>
                                        5人点评
                                    </a>
								</span>
                                                                
                                                                
                                                                <span class="infoSide">
                                                                    普拉兰岛
                                                                景点观光综合排名 <em class="rank orange">第3位</em>
                                </span>
                                							</div>
                            
							<div class="comment clearfix">
                                								<p class="user">
                                    <a href="//www.qyer.com/u/8838326" target='_blank'>
                                        <img src="" width="32" height="32">
                                    </a>
                                </p>
                                <div class="txt">
                                    好想去啊  看着美丽的海滩 清澈的大海 湛蓝的天空 已经神游~~~  等我

                                </div>
								                                
                                							</div>
                            
                            
                            <p data-ipg-add='place-poilist-wishto-add' data-ipg-delete='place-poilist-wishto-delete' class="plcAddToLikeList _addToLikeList" data-status="1" data-pid="123408" data-type="2" data-userid="0"></p>
                            <p data-bn-ipg='place-poilist-poi-addtoplan-1' class="goToMap" title="查看地图" data-pid="123408" data-type="poi"></p>
						</div>
					</li>
                                        <li class="clearfix">
						<div class="cntBox clearfix">
							<p class="pics">
								<a href="//place.qyer.com/poi/V2UJa1FhBzRTbVI_/" target='_blank'>
									<img src="http://pic.qyer.com/album/user/1870/35/QEFSQhkCZ04/index/225x150" width="227" height="150">
                                    								</a>
							</p>
                            
							<h3 class="title fontYaHei">
								<a href="//place.qyer.com/poi/V2UJa1FhBzRTbVI_/" target='_blank'>
                                                                            圣皮耶尔岛
                                                                        &nbsp;&nbsp;
                                    <span>St. Pierre Island</span>
                                </a>
							</h3>
                            
							<div class="info">
                                								<p class="stars">
                                    <span class='star'></span><span class='star'></span><span class='star'></span><span class='star'></span><span class='null'></span>
								</p>
								<span class="grade">8.0</span>
								<span class="dping">
									<a href="//place.qyer.com/poi/V2UJa1FhBzRTbVI_/review/" target='_blank'>
                                        3人点评
                                    </a>
								</span>
                                                                
                                                                
                                                                <span class="infoSide">
                                                                    普拉兰岛
                                                                景点观光综合排名 <em class="rank orange">第7位</em>
                                </span>
                                							</div>
                            
							<div class="comment clearfix">
                                								<p class="user">
                                    <a href="//www.qyer.com/u/436466" target='_blank'>
                                        <img src="" width="32" height="32">
                                    </a>
                                </p>
                                <div class="txt">
                                    塞舌尔的标志，有海山黄山之称，值得一去。参加一日游，很多人来浮潜，但没啥鱼。

                                </div>
								                                
                                							</div>
                            
                            
                            <p data-ipg-add='place-poilist-wishto-add' data-ipg-delete='place-poilist-wishto-delete' class="plcAddToLikeList _addToLikeList" data-status="1" data-pid="187290" data-type="2" data-userid="0"></p>
                            <p data-bn-ipg='place-poilist-poi-addtoplan-1' class="goToMap" title="查看地图" data-pid="187290" data-type="poi"></p>
						</div>
					</li>
                                        <li class="clearfix">
						<div class="cntBox clearfix">
							<p class="pics">
								<a href="//place.qyer.com/poi/V2UJYlFhBzNTZ1I6/" target='_blank'>
									<img src="http://pic1.qyer.com/album/user/945/31/SE1QQRsDZg/index/225x150" width="227" height="150">
                                    								</a>
							</p>
                            
							<h3 class="title fontYaHei">
								<a href="//place.qyer.com/poi/V2UJYlFhBzNTZ1I6/" target='_blank'>
                                                                            ZIMBABWE
                                                                        &nbsp;&nbsp;
                                    <span></span>
                                </a>
							</h3>
                            
							<div class="info">
                                								<p class="stars">
                                    <span class='star'></span><span class='star'></span><span class='star'></span><span class='star'></span><span class='null'></span>
								</p>
								<span class="grade">8.0</span>
								<span class="dping">
									<a href="//place.qyer.com/poi/V2UJYlFhBzNTZ1I6/review/" target='_blank'>
                                        5人点评
                                    </a>
								</span>
                                                                
                                                                
                                                                <span class="infoSide">
                                                                    普拉兰岛
                                                                景点观光综合排名 <em class="rank orange">第8位</em>
                                </span>
                                							</div>
                            
							<div class="comment clearfix">
                                								<p class="user">
                                    <a href="//www.qyer.com/u/2717100" target='_blank'>
                                        <img src="" width="32" height="32">
                                    </a>
                                </p>
                                <div class="txt">
                                    在拉齐奥海滩附近的山头上，登高望远，本身并没有太多亮点，时间原因没能在这里看日落。

                                </div>
								                                
                                							</div>
                            
                            
                            <p data-ipg-add='place-poilist-wishto-add' data-ipg-delete='place-poilist-wishto-delete' class="plcAddToLikeList _addToLikeList" data-status="1" data-pid="117535" data-type="2" data-userid="0"></p>
                            <p data-bn-ipg='place-poilist-poi-addtoplan-1' class="goToMap" title="查看地图" data-pid="117535" data-type="poi"></p>
						</div>
					</li>
                                        <li class="clearfix">
						<div class="cntBox clearfix">
							<p class="pics">
								<a href="//place.qyer.com/poi/V2YJY1FmBz5TZ1I-/" target='_blank'>
									<img src="http://pic1.qyer.com/album/user/1864/29/QEFTRhgOZko/index/225x150" width="227" height="150">
                                    								</a>
							</p>
                            
							<h3 class="title fontYaHei">
								<a href="//place.qyer.com/poi/V2YJY1FmBz5TZ1I-/" target='_blank'>
                                                                            Anse Georgette
                                                                        &nbsp;&nbsp;
                                    <span></span>
                                </a>
							</h3>
                            
							<div class="info">
                                								<p class="stars">
                                    <span class='star'></span><span class='star'></span><span class='star'></span><span class='star'></span><span class='half'></span>
								</p>
								<span class="grade">9.0</span>
								<span class="dping">
									<a href="//place.qyer.com/poi/V2YJY1FmBz5TZ1I-/review/" target='_blank'>
                                        4人点评
                                    </a>
								</span>
                                                                
                                                                
                                                                <span class="infoSide">
                                                                    普拉兰岛
                                                                景点观光综合排名 <em class="rank orange">第4位</em>
                                </span>
                                							</div>
                            
							<div class="comment clearfix">
                                								<p class="user">
                                    <a href="//www.qyer.com/u/2717100" target='_blank'>
                                        <img src="" width="32" height="32">
                                    </a>
                                </p>
                                <div class="txt">
                                    普拉兰岛最好的海滩之一。在Constance Lemuria Resort酒店里，是三个岛上唯一需要预约才能进入的海滩。糖粉白沙，配上果冻海水，何况站在海里，还能看见魔鬼鱼和其他不知名的热带鱼，堪称完美。虽然徒步前往要翻过一座小山坡，不过路边是景色宜人的高尔夫球场，间或还有几颗海椰子树点缀其间，累也值得了。有时候浪会比较大，水性不好的同学请注意，本人太阳镜就葬身于此。

                                </div>
								                                
                                                                <p class="mguideNumber">
                                    <a href="javascript:void(0)" data-id='200831' data-bn-ipg='poilist-poi-mguide-6'>
                                        1个微锦囊推荐了这里
                                    </a>
                                </p>
                                							</div>
                            
                            
                            <p data-ipg-add='place-poilist-wishto-add' data-ipg-delete='place-poilist-wishto-delete' class="plcAddToLikeList _addToLikeList" data-status="1" data-pid="200831" data-type="2" data-userid="0"></p>
                            <p data-bn-ipg='place-poilist-poi-addtoplan-1' class="goToMap" title="查看地图" data-pid="200831" data-type="poi"></p>
						</div>
					</li>
                                        <li class="clearfix">
						<div class="cntBox clearfix">
							<p class="pics">
								<a href="//place.qyer.com/poi/V2YJY1FmBz5TZ1I8/" target='_blank'>
									<img src="http://pic.qyer.com/album/user/1139/79/QEhWSx0OaE4/index/225x150" width="227" height="150">
                                    								</a>
							</p>
                            
							<h3 class="title fontYaHei">
								<a href="//place.qyer.com/poi/V2YJY1FmBz5TZ1I8/" target='_blank'>
                                                                            Anse Volbert
                                                                        &nbsp;&nbsp;
                                    <span></span>
                                </a>
							</h3>
                            
							<div class="info">
                                								<p class="stars">
                                    <span class='star'></span><span class='star'></span><span class='star'></span><span class='star'></span><span class='half'></span>
								</p>
								<span class="grade">9.0</span>
								<span class="dping">
									<a href="//place.qyer.com/poi/V2YJY1FmBz5TZ1I8/review/" target='_blank'>
                                        4人点评
                                    </a>
								</span>
                                                                
                                                                
                                                                <span class="infoSide">
                                                                    普拉兰岛
                                                                景点观光综合排名 <em class="rank orange">第5位</em>
                                </span>
                                							</div>
                            
							<div class="comment clearfix">
                                								<p class="user">
                                    <a href="//www.qyer.com/u/2053977" target='_blank'>
                                        <img src="" width="32" height="32">
                                    </a>
                                </p>
                                <div class="txt">
                                    海滩很漂亮 海滩边的takeaway便宜又好吃！就是狗有点吓人

                                </div>
								                                
                                                                <p class="mguideNumber">
                                    <a href="javascript:void(0)" data-id='200833' data-bn-ipg='poilist-poi-mguide-7'>
                                        1个微锦囊推荐了这里
                                    </a>
                                </p>
                                							</div>
                            
                            
                            <p data-ipg-add='place-poilist-wishto-add' data-ipg-delete='place-poilist-wishto-delete' class="plcAddToLikeList _addToLikeList" data-status="1" data-pid="200833" data-type="2" data-userid="0"></p>
                            <p data-bn-ipg='place-poilist-poi-addtoplan-1' class="goToMap" title="查看地图" data-pid="200833" data-type="poi"></p>
						</div>
					</li>
                                        <li class="clearfix">
						<div class="cntBox clearfix">
							<p class="pics">
								<a href="//place.qyer.com/poi/V2YJY1FmBz5TZ1I7/" target='_blank'>
									<img src="http://pic.qyer.com/album/user/1581/62/QExdQxwFZUg/index/225x150" width="227" height="150">
                                    								</a>
							</p>
                            
							<h3 class="title fontYaHei">
								<a href="//place.qyer.com/poi/V2YJY1FmBz5TZ1I7/" target='_blank'>
                                                                            A. Takamaka
                                                                        &nbsp;&nbsp;
                                    <span></span>
                                </a>
							</h3>
                            
							<div class="info">
                                								<p class="stars">
                                    <span class='star'></span><span class='star'></span><span class='star'></span><span class='star'></span><span class='half'></span>
								</p>
								<span class="grade">9.5</span>
								<span class="dping">
									<a href="//place.qyer.com/poi/V2YJY1FmBz5TZ1I7/review/" target='_blank'>
                                        4人点评
                                    </a>
								</span>
                                                                
                                                                
                                                                <span class="infoSide">
                                                                    普拉兰岛
                                                                景点观光综合排名 <em class="rank orange">第6位</em>
                                </span>
                                							</div>
                            
							<div class="comment clearfix">
                                								<p class="user">
                                    <a href="//www.qyer.com/u/7635620" target='_blank'>
                                        <img src="" width="32" height="32">
                                    </a>
                                </p>
                                <div class="txt">
                                    幸运的能够入住这间久负盛名的酒店，沙滩很安静，海水清澈，适合浮潜，能看到好多美丽的小鱼。塞舌尔的沙滩沙质普遍都是细白的像精品面粉，这里也不例外。

                                </div>
								                                
                                                                <p class="mguideNumber">
                                    <a href="javascript:void(0)" data-id='200834' data-bn-ipg='poilist-poi-mguide-8'>
                                        1个微锦囊推荐了这里
                                    </a>
                                </p>
                                							</div>
                            
                            
                            <p data-ipg-add='place-poilist-wishto-add' data-ipg-delete='place-poilist-wishto-delete' class="plcAddToLikeList _addToLikeList" data-status="1" data-pid="200834" data-type="2" data-userid="0"></p>
                            <p data-bn-ipg='place-poilist-poi-addtoplan-1' class="goToMap" title="查看地图" data-pid="200834" data-type="poi"></p>
						</div>
					</li>
                                        <li class="clearfix">
						<div class="cntBox clearfix">
							<p class="pics">
								<a href="//place.qyer.com/poi/V2YJY1FmBz5TZ1I9/" target='_blank'>
									<img src="http://pic1.qyer.com/album/user/991/88/SEBUShIAZg/index/225x150" width="227" height="150">
                                    								</a>
							</p>
                            
							<h3 class="title fontYaHei">
								<a href="//place.qyer.com/poi/V2YJY1FmBz5TZ1I9/" target='_blank'>
                                                                            Anse La Blague
                                                                        &nbsp;&nbsp;
                                    <span></span>
                                </a>
							</h3>
                            
							<div class="info">
                                								<p class="stars">
                                    <span class='star'></span><span class='star'></span><span class='star'></span><span class='null'></span><span class='null'></span>
								</p>
								<span class="grade">6.0</span>
								<span class="dping">
									<a href="//place.qyer.com/poi/V2YJY1FmBz5TZ1I9/review/" target='_blank'>
                                        1人点评
                                    </a>
								</span>
                                                                
                                                                
                                                                <span class="infoSide">
                                                                    普拉兰岛
                                                                景点观光综合排名 <em class="rank orange">第10位</em>
                                </span>
                                							</div>
                            
							<div class="comment clearfix">
                                								<p class="user">
                                    <a href="//www.qyer.com/u/2717100" target='_blank'>
                                        <img src="" width="32" height="32">
                                    </a>
                                </p>
                                <div class="txt">
                                    虽然不是知名海滩，却也水清沙幼，胜在清静。这个海滩在一条小路的尽头，去的时候只有我们一家人。海滩浪比较小，适合玩水。

                                </div>
								                                
                                                                <p class="mguideNumber">
                                    <a href="javascript:void(0)" data-id='200832' data-bn-ipg='poilist-poi-mguide-9'>
                                        1个微锦囊推荐了这里
                                    </a>
                                </p>
                                							</div>
                            
                            
                            <p data-ipg-add='place-poilist-wishto-add' data-ipg-delete='place-poilist-wishto-delete' class="plcAddToLikeList _addToLikeList" data-status="1" data-pid="200832" data-type="2" data-userid="0"></p>
                            <p data-bn-ipg='place-poilist-poi-addtoplan-1' class="goToMap" title="查看地图" data-pid="200832" data-type="poi"></p>
						</div>
					</li>
                                        <li class="clearfix">
						<div class="cntBox clearfix">
							<p class="pics">
								<a href="//place.qyer.com/poi/V2YJY1FmBz5TZ1I5/" target='_blank'>
									<img src="http://pic.qyer.com/album/user/991/95/SEBUSx8DZw/index/225x150" width="227" height="150">
                                    								</a>
							</p>
                            
							<h3 class="title fontYaHei">
								<a href="//place.qyer.com/poi/V2YJY1FmBz5TZ1I5/" target='_blank'>
                                                                            Anse Boudin
                                                                        &nbsp;&nbsp;
                                    <span></span>
                                </a>
							</h3>
                            
							<div class="info">
                                								<p class="stars">
                                    <span class='star'></span><span class='star'></span><span class='star'></span><span class='star'></span><span class='null'></span>
								</p>
								<span class="grade">8.0</span>
								<span class="dping">
									<a href="//place.qyer.com/poi/V2YJY1FmBz5TZ1I5/review/" target='_blank'>
                                        2人点评
                                    </a>
								</span>
                                                                
                                                                
                                                                <span class="infoSide">
                                                                    普拉兰岛
                                                                景点观光综合排名 <em class="rank orange">第9位</em>
                                </span>
                                							</div>
                            
							<div class="comment clearfix">
                                								<p class="user">
                                    <a href="//www.qyer.com/u/2717100" target='_blank'>
                                        <img src="" width="32" height="32">
                                    </a>
                                </p>
                                <div class="txt">
                                    去往拉齐奥海滩的必经之处。岸边的礁石和枯枝是这一片小海滩别有情趣，这里也是普岛最好的浮潜点之一。

                                </div>
								                                
                                                                <p class="mguideNumber">
                                    <a href="javascript:void(0)" data-id='200836' data-bn-ipg='poilist-poi-mguide-10'>
                                        1个微锦囊推荐了这里
                                    </a>
                                </p>
                                							</div>
                            
                            
                            <p data-ipg-add='place-poilist-wishto-add' data-ipg-delete='place-poilist-wishto-delete' class="plcAddToLikeList _addToLikeList" data-status="1" data-pid="200836" data-type="2" data-userid="0"></p>
                            <p data-bn-ipg='place-poilist-poi-addtoplan-1' class="goToMap" title="查看地图" data-pid="200836" data-type="poi"></p>
						</div>
					</li>
                                        <li class="clearfix">
						<div class="cntBox clearfix">
							<p class="pics">
								<a href="//place.qyer.com/poi/V2UJYlFgBzVTbVI8Cms/" target='_blank'>
									<img src="http://pic.qyer.com/album/user/2559/81/Q0xQSxIGZko/index/225x150" width="227" height="150">
                                    								</a>
							</p>
                            
							<h3 class="title fontYaHei">
								<a href="//place.qyer.com/poi/V2UJYlFgBzVTbVI8Cms/" target='_blank'>
                                                                            库金岛特别保护区
                                                                        &nbsp;&nbsp;
                                    <span>Cousin Island Special Reserve</span>
                                </a>
							</h3>
                            
							<div class="info">
                                								<p class="stars">
                                    <span class='star'></span><span class='star'></span><span class='star'></span><span class='star'></span><span class='star'></span>
								</p>
								<span class="grade">10.0</span>
								<span class="dping">
									<a href="//place.qyer.com/poi/V2UJYlFgBzVTbVI8Cms/review/" target='_blank'>
                                        1人点评
                                    </a>
								</span>
                                                                
                                                                
                                                                <span class="infoSide">
                                                                    普拉兰岛
                                                                景点观光综合排名 <em class="rank orange">第13位</em>
                                </span>
                                							</div>
                            
							<div class="comment clearfix">
                                								<p class="user">
                                    <a href="//www.qyer.com/u/2087099" target='_blank'>
                                        <img src="" width="32" height="32">
                                    </a>
                                </p>
                                <div class="txt">
                                    去的时候是10月份旱季，岛上并没有很多蚊子，鸟的种类不是很多，但数量很多，并且能近距离的观赏，还有好多巨龟~~~

                                </div>
								                                
                                							</div>
                            
                            
                            <p data-ipg-add='place-poilist-wishto-add' data-ipg-delete='place-poilist-wishto-delete' class="plcAddToLikeList _addToLikeList" data-status="1" data-pid="1163937" data-type="2" data-userid="0"></p>
                            <p data-bn-ipg='place-poilist-poi-addtoplan-1' class="goToMap" title="查看地图" data-pid="1163937" data-type="poi"></p>
						</div>
					</li>
                                        <li class="clearfix">
						<div class="cntBox clearfix">
							<p class="pics">
								<a href="//place.qyer.com/poi/V2UJYlFgBzVTbVI8Cm8/" target='_blank'>
									<img src="//static.qyer.com/images/place/no/300x200/poi_151.jpg" width="227" height="150">
                                    								</a>
							</p>
                            
							<h3 class="title fontYaHei">
								<a href="//place.qyer.com/poi/V2UJYlFgBzVTbVI8Cm8/" target='_blank'>
                                                                            Anse Marie-Louise
                                                                        &nbsp;&nbsp;
                                    <span></span>
                                </a>
							</h3>
                            
							<div class="info">
                                                                暂无点评
                                                                
                                                                
                                                                <span class="infoSide">
                                                                    普拉兰岛
                                                                景点观光综合排名 <em class="rank orange">第11位</em>
                                </span>
                                							</div>
                            
							<div class="comment clearfix">
                                                                <div class="txt">
                                    玛丽-路易海湾是普拉兰岛最东南边的海湾，距圣安妮海湾码头仅1公里远。在玛丽-路易海湾以西还有几处小海湾，是普拉兰岛南部为数不多的景色值得称道的区域，建议自驾的旅行者在此停车拍照。

                                </div>
                                                                
                                							</div>
                            
                            
                            <p data-ipg-add='place-poilist-wishto-add' data-ipg-delete='place-poilist-wishto-delete' class="plcAddToLikeList _addToLikeList" data-status="1" data-pid="1163933" data-type="2" data-userid="0"></p>
                            <p data-bn-ipg='place-poilist-poi-addtoplan-1' class="goToMap" title="查看地图" data-pid="1163933" data-type="poi"></p>
						</div>
					</li>
                                        <li class="clearfix">
						<div class="cntBox clearfix">
							<p class="pics">
								<a href="//place.qyer.com/poi/V2UJYlFgBzVTbVI7Cmw/" target='_blank'>
									<img src="//static.qyer.com/images/place/no/300x200/poi_151.jpg" width="227" height="150">
                                    								</a>
							</p>
                            
							<h3 class="title fontYaHei">
								<a href="//place.qyer.com/poi/V2UJYlFgBzVTbVI7Cmw/" target='_blank'>
                                                                            库瑞尔岛海洋国家公园
                                                                        &nbsp;&nbsp;
                                    <span>Curieuse Marine National Park</span>
                                </a>
							</h3>
                            
							<div class="info">
                                                                暂无点评
                                                                
                                                                
                                                                <span class="infoSide">
                                                                    普拉兰岛
                                                                景点观光综合排名 <em class="rank orange">第12位</em>
                                </span>
                                							</div>
                            
							<div class="comment clearfix">
                                                                <div class="txt">
                                    库瑞尔岛海洋国家公园的范围覆盖了由普拉兰岛东北海域至库瑞尔岛的广大区域，其核心为位于普拉兰岛东北海岸约1.5公里外的库瑞尔岛。岛上有一个巨大的象龟饲养研究中心，几百只象龟被自由散养，旅行者可以与它们近距离接触，并给它们喂食。此外，岛上还有几处人迹罕至的秀美海湾供旅行者探索。

                                </div>
                                                                
                                							</div>
                            
                            
                            <p data-ipg-add='place-poilist-wishto-add' data-ipg-delete='place-poilist-wishto-delete' class="plcAddToLikeList _addToLikeList" data-status="1" data-pid="1163940" data-type="2" data-userid="0"></p>
                            <p data-bn-ipg='place-poilist-poi-addtoplan-1' class="goToMap" title="查看地图" data-pid="1163940" data-type="poi"></p>
						</div>
					</li>
                                        <li class="clearfix">
						<div class="cntBox clearfix">
							<p class="pics">
								<a href="//place.qyer.com/poi/V2UJYlFgBzVTbVI7Cm8/" target='_blank'>
									<img src="//static.qyer.com/images/place/no/300x200/poi_151.jpg" width="227" height="150">
                                    								</a>
							</p>
                            
							<h3 class="title fontYaHei">
								<a href="//place.qyer.com/poi/V2UJYlFgBzVTbVI7Cm8/" target='_blank'>
                                                                             St Pierre Island
                                                                        &nbsp;&nbsp;
                                    <span></span>
                                </a>
							</h3>
                            
							<div class="info">
                                                                暂无点评
                                                                
                                                                
                                                                <span class="infoSide">
                                                                    普拉兰岛
                                                                景点观光综合排名 <em class="rank orange">第14位</em>
                                </span>
                                							</div>
                            
							<div class="comment clearfix">
                                                                <div class="txt">
                                    这座天堂般的小岛位于沃伯特海湾东北方向约1公里处，岛屿周围的海水清澈透亮，海洋生物丰富，是浮潜的好去处。岛上生长着与黄山迎客松非常形似的不知名树木，二者结合便产生了“黄山出于海上”的幻觉。

                                </div>
                                                                
                                							</div>
                            
                            
                            <p data-ipg-add='place-poilist-wishto-add' data-ipg-delete='place-poilist-wishto-delete' class="plcAddToLikeList _addToLikeList" data-status="1" data-pid="1163943" data-type="2" data-userid="0"></p>
                            <p data-bn-ipg='place-poilist-poi-addtoplan-1' class="goToMap" title="查看地图" data-pid="1163943" data-type="poi"></p>
						</div>
					</li>
                                        <li class="clearfix">
						<div class="cntBox clearfix">
							<p class="pics">
								<a href="//place.qyer.com/poi/V2UJYlFgBzVTbVI8Cmg/" target='_blank'>
									<img src="//static.qyer.com/images/place/no/300x200/poi_151.jpg" width="227" height="150">
                                    								</a>
							</p>
                            
							<h3 class="title fontYaHei">
								<a href="//place.qyer.com/poi/V2UJYlFgBzVTbVI8Cmg/" target='_blank'>
                                                                            塔卡马卡海湾 
                                                                        &nbsp;&nbsp;
                                    <span>Anse Takamaka</span>
                                </a>
							</h3>
                            
							<div class="info">
                                                                暂无点评
                                                                
                                                                
                                                                <span class="infoSide">
                                                                    普拉兰岛
                                                                景点观光综合排名 <em class="rank orange">第15位</em>
                                </span>
                                							</div>
                            
							<div class="comment clearfix">
                                                                <div class="txt">
                                    普拉兰岛上有两处塔卡马卡海滩，分别位于岛屿的东北部与西南部，大多数旅行者前往的是位于东北部的塔卡马卡海湾。该海湾的海水颜色为翠绿色，而且绿得十分纯粹，适合登高摄影，普拉兰岛莱佛士酒店（Raffles Praslin）就位于海湾的山坡上。

                                </div>
                                                                
                                							</div>
                            
                            
                            <p data-ipg-add='place-poilist-wishto-add' data-ipg-delete='place-poilist-wishto-delete' class="plcAddToLikeList _addToLikeList" data-status="1" data-pid="1163934" data-type="2" data-userid="0"></p>
                            <p data-bn-ipg='place-poilist-poi-addtoplan-1' class="goToMap" title="查看地图" data-pid="1163934" data-type="poi"></p>
						</div>
					</li>
                                    </ul>
                                
                <div class="plcPoiListPage" id="poiListPage">
                    
                </div>
            </div>
            <!-- 游记攻略 end -->
            
        </div>
        
        <div class="qySide fr">
            
            
            <div class="plcLxdMaps">
    <h3 class="subTitle fontYaHei">普拉兰岛景点地图</h3>
    <div class="mapbox">
    	<a href="//place.qyer.com/praslin-island/map/sight/" data-bn-ipg="place-poilist-map" target="_blank">
    		<img src="https://pic1.qyer.com/poi/lists/googlemap/642cd8a1a48f7a822c0f4b1be2c8484c_266x180.png?time=1511439014" id="sidebarSmallMapImg">
    		<span class="txt">查看地图</span>
    	</a>
    </div>
</div>
                        <p class="plaBorderGap"></p>
            
            <!-- 穷游锦囊 before -->
                        <div class="plcSideMguide" id="qyGuide">
                <p class="plcSideMguideTitle fontYaHei">穷游锦囊</p>
                <p class="plcSideMguidePage"><span class="prev" title="上一个" data-bn-ipg="place-poilist-guide-paging"></span><span class="page"><a href="javascript:void(0);" class="js-guideBatchDown" data-bn-ipg="place-poilist-guide-all"><em>1</em> / 1 本</a></span><span class="next" title="下一个" data-bn-ipg="place-poilist-guide-paging"></span></p>
                <ul class="plcSideMguideContent">
                                        <li data-id="618">
                        <p class="cover">
                            <a href="//guide.qyer.com/seychelles/" class="_jsjndown" data-id="618" data-bn-ipg='place-poilist-guide-pic' target="_blank">
                                <img src="//guidepic.qyerstatic.com/upload/Guide_Info/eb/6f/618/100_150.jpg?1496833908" width="60" height="90" alt="" />
                            </a>
                        </p>
                        <p class="title fontYaHei">
                            <a href="//guide.qyer.com/seychelles/" class="_jsjndown" data-id="" data-bn-ipg='place-poilist-guide-name' target="_blank">
                                塞舌尔
                            </a>
                        </p>
                        <p class="info">作者：startzb<br />
                        下载次数：15958</p>
                        <p class="download"><a href="javascript:void(0);" class="_jsjndown ui_buttonA" data-id="618" data-bn-ipg='place-poilist-guide-download' class="">免费下载</a></p>
                    </li>
                                    </ul>
            </div>
            <!-- 穷游锦囊 end -->

            <p class="plaBorderGap"></p>
            
                        <div class="plcTjDest">
                <h3 class="title fontYaHei">想推荐普拉兰岛的旅行地？</h3>
                <div class="btns">
                    <a class="ui_buttonA fl" href="/mguide/add" data-bn-ipg="place-poilist-add-mguide" target="_blank">创建微锦囊</a>
                    <a class="ui_buttonA fr" href="//place.qyer.com/praslin-island/sight/add/" data-bn-ipg="place-poilist-add-poi"target="_blank">创建旅行地</a>
                </div>
            </div>

            <!-- <p class="plaBorderGap"></p> -->
                    </div>
        <div class="cb"></div>
    </div>
</div>


<!-- 游记攻略列表模块 before -->
<script id="poilistTmpl" type="text/html">
{@each list as it,totalIndex}
<li data-id="${it.id}">
	<div class="cntBox clearfix">
		<p class="pics"><a href="${it.url}" target='_blank'><img src="${it.photo}" width="227" height="150">{@if it.hotgrade == 1}<span class="label">用户热推</span>{@/if}</a></p>

		<h3 class="title fontYaHei"><a href="${it.url}" target='_blank'>${it.cnname}&nbsp;&nbsp;<span>${it.enname}</span></a></h3>

		<div class="info">
			<p class="stars">$${it.grade | commentStar}</p>
			<span class="grade">${it.grade}</span><span class="dping"><a href="${it.commentUrl}">${it.commentCount}人点评</a></span>
			{@if it.lastmin == true}<span class="zhe">折</span>{@/if}
			
			{@if it.rank > 0}
			<span class="infoSide">
				{@if it.cityname}${it.cityname}{@/if}${it.catename}排名 <em class="rank orange">第${it.rank}位</em>
			</span>
			{@/if}
		</div>
		{@each it.comments as it2, index}
		{@if index == 0}
		<div class="comment clearfix">
			<p class="user"><a href="${it2.userLink}"><img src="${it2.userPhoto}" width="32" height="32" alt="${it2.author}"></a></p>
			<p class="txt">${it2.text}</p>
			{@if it.mguideCount > 0}<p class="mguideNumber"><a data-id="${it.id}" data-bn-ipg="place-poilist-poi-mguide-${totalIndex}" href="javascript:void(0);">${it.mguideCount}个微锦囊推荐了这里</a></p>{@/if}
		</div>
		{@/if}
		{@/each}

		{@if it.comments.length == 0}
		<div class="comment clearfix">
			<p class="txt">${it.introduction}</p>
		</div>
		{@/if}
        
        {@if it.discount.length > 0}
        <dl class="discount">
            <dt class="orange">优选折扣:</dt>
            <dd class="list">
                {@each it.discount as it2, index}
                <p>
                    <a target='_blank' href="${it2.discountLink}" data-bn-ipg="place-poilist-poi-lm${totalIndex}-${index}">${it2.discountTitle}</a>
                </p>
                {@/each}
            </dd>
            {@if it.discount.length > 1}
            <dd class="arrowUp _arrow" data-bn-ipg="place-poilist-poi-lm-more"></dd>
            {@/if}
        </dl>
        {@/if}

        {@if it.ShopDiscDetail}
        <dl class="discount shopping">
            <dt class="orange">购物优惠</dt>
            <dd class="list">
                $${it.ShopDiscDetail}
            </dd>
            <dd class="_arrow arrowUp"></dd>
        </dl>
        {@/if}

        <p data-ipg-add='place-poilist-wishto-add' data-ipg-delete='place-poilist-wishto-delete' class="{@if it.planto_go==1}plcCancelToLikeList{@else}plcAddToLikeList{@/if} _addToLikeList" data-status="{@if it.planto_go==1}0{@else}1{@/if}" data-pid="${it.id}" data-type="2"></p>
		<p class="goToMap" data-bn-ipg="place-poilist-poi-addtoplan-1" title="显示周边" data-pid="${it.id}" data-type="poi"></p>
	</div>
</li>
{@/each}
</script>
<!-- 游记攻略列表模块 end -->


<div style="display: none;">http://place.qyer.com/praslin-island/</div>
<div class="qyer_footer">
	<div class="topline"></div>
	<div class="content">
		<p class="nav">
			<a href="//www.qyer.com/htmlpages/about.html" target="_blank" rel="nofollow" data-bn-ipg="foot-about-1">穷游简介</a>
			<a href="//www.qyer.com/partner/" target="_blank" rel="nofollow" data-bn-ipg="foot-about-3">合作伙伴</a>
			<a href="//www.qyer.com/job/" target="_blank" rel="nofollow" data-bn-ipg="foot-join-1">加入我们</a>
			<a href="//site.qyer.com/tyro/" target="_blank" rel="nofollow" data-bn-ipg="foot-help-1">新手上路</a>
			<a href="//bbs.qyer.com/forum-10-1.html" target="_blank" rel="nofollow" data-bn-ipg="foot-help-2">使用帮助</a>
			<a href="//www.qyer.com/sitemap.html" target="_blank" data-bn-ipg="foot-help-4">网站地图</a>
			<a href="//www.qyer.com/htmlpages/member.html" target="_blank" rel="nofollow" data-bn-ipg="foot-clause-1">会员条款</a>
			<a href="//www.qyer.com/htmlpages/bbsguide.html" target="_blank" rel="nofollow" data-bn-ipg="foot-clause-2">社区指南</a>
			<a href="//www.qyer.com/htmlpages/copyright.html" target="_blank" rel="nofollow" data-bn-ipg="foot-clause-3">版权声明</a>
			<a href="//www.qyer.com/htmlpages/exemption.html" target="_blank" rel="nofollow" data-bn-ipg="foot-clause-4">免责声明</a>
			<a href="//www.qyer.com/htmlpages/contact.html" target="_blank" rel="nofollow" data-bn-ipg="foot-about-2">联系我们</a>
		</p>
		<p class="info">
			2004-2017 &copy; 穷游网&reg;  qyer.com All rights reserved. Version v5.57  京ICP备12003524号  京公网安备11010102001935号  京ICP证140673号
			<a target="_blank" style="color:inherit;" href="//static.qyer.com/images/yyzz.jpg">营业执照</a>
			<a target="_blank" style="color:inherit;" href="//static.qyer.com/images/jyxkz.jpg">经营许可证</a>
		</p>
    
        <!--友情链接模块-->
        
	</div>
</div>



<script>
        var script   = document.createElement("script");
        script.type  = "text/javascript";
        script.async = true;
        script.src   = "//static.qyer.com/qyer_ui_frame/m/js/dist/base_beacon_ga.js";
        document.getElementsByTagName("head")[0].appendChild(script);
</script>

<script>
window.QYER = window.QYER || {};
window.QYER.uid = 0;
window._qyer_userid = 0;
</script>

</body>
</html>
'''
doc = pyquery.PyQuery(content)
from service_platform_conn_pool import service_platform_pool

conn = service_platform_pool.connection()
cursor = conn.cursor()
for line in doc("#poiLists>li>div"):
    div = pyquery.PyQuery(line)
    # data_list = root.xpath('//ul[@id="poiLists"]/li/div/h3/a/@href')
    # id_list = root.xpath('//ul[@id="poiLists"]/li/div/p[@data-type="poi"]/@data-pid')
    # data_name = root.xpath('//ul[@id="poiLists"]/li/div/h3/a/text()')
    # data_list = map(lambda x: urljoin('http:', x), data_list)

    print(div("p[data-type='poi']").attr['data-pid'], 'http' + div("h3>a").attr.href)
    cursor.execute(
        'insert into list_total_qyer_20171120a(`source`,`source_id`,`city_id`,`country_id`,`hotel_url`) VALUES (%s, %s, %s, %s, %s)',
        ('qyer', div("p[data-type='poi']").attr['data-pid'], '40051', '412', 'http' + div("h3>a").attr.href))
conn.commit()
