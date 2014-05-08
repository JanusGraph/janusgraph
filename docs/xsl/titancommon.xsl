<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

  <xsl:template name="titan.head">

    <script type='text/javascript' src='http://intelliscence.com/web/work/demos/titan/wp-includes/js/jquery/jquery.js?ver=1.11.0'></script>
    <script type='text/javascript' src='http://intelliscence.com/web/work/demos/titan/wp-includes/js/jquery/jquery-migrate.min.js?ver=1.2.1'></script>

    <!--[if lte IE 8]>
    <link rel="stylesheet" href="http://intelliscence.com/web/work/demos/titan/wp-content/themes/Avada/css/ie8.css" />
    <![endif]-->

    <!--[if IE]>
    <link rel="stylesheet" href="http://intelliscence.com/web/work/demos/titan/wp-content/themes/Avada/css/ie.css" />
    <![endif]-->

    <link rel='stylesheet' id='style-css-css'  href='http://intelliscence.com/web/work/demos/titan/wp-content/themes/Avada/style.css?ver=3.9' type='text/css' media='all' />
    <link rel='stylesheet' id='media-css-css'  href='http://intelliscence.com/web/work/demos/titan/wp-content/themes/Avada/css/media.css' type='text/css' media='all' />
    <link rel='stylesheet' id='animate-css-css'  href='http://intelliscence.com/web/work/demos/titan/wp-content/themes/Avada/css/animate-custom.css' type='text/css' media='all' />
    <link rel='stylesheet' id='inline-blob-from-avada-prototype' href='css/titanblob.css' type='text/css' media='all' />

  </xsl:template>

  <xsl:template name="titan.body">
    <xsl:param name="maincontent"/>
    <body>
      <xsl:call-template name="body.attributes"/>
      <div id="wrapper" >
	<div class="header-wrapper">
	  <div class="header-v1">
	    <header id="header">
	      <div class="avada-row" style="margin-top:0px;margin-bottom:0px;">
		<div class="logo" style="margin-right:0px;margin-top:31px;margin-left:0px;margin-bottom:31px;">
		  <a href="http://intelliscence.com/web/work/demos/titan">
		    <img src="http://intelliscence.com/web/work/demos/titan/wp-content/uploads/2014/04/main-logo3.png" alt="Titan" class="normal_logo" />
		  </a>
		</div>
		<nav id="nav" class="nav-holder">
		  <ul class="navigation menu fusion-navbar-nav">
		    <li id="menu-item-4678" class="menu-item menu-item-type-custom menu-item-object-custom menu-item-4678"><a    href="#">Home</a></li>
		    <li id="menu-item-4707" class="menu-item menu-item-type-custom menu-item-object-custom menu-item-4707"><a    href="#">Learn</a></li>
		    <li id="menu-item-4708" class="menu-item menu-item-type-custom menu-item-object-custom menu-item-4708"><a    href="#">Showcase</a></li>
		    <li id="menu-item-4709" class="menu-item menu-item-type-custom menu-item-object-custom menu-item-4709"><a    href="#">Support</a></li>
		    <li id="menu-item-4710" class="menu-item menu-item-type-custom menu-item-object-custom menu-item-4710"><a    href="#">Download</a></li>
		    <li id="menu-item-4711" class="menu-item menu-item-type-post_type menu-item-object-page current-menu-item page_item page-item-3610 current_page_item menu-item-4711"><a    href="#">Documentation</a></li>
		    <li id="menu-item-4712" class="menu-item menu-item-type-custom menu-item-object-custom menu-item-4712"><a    href="#">Blog &#038; Events</a></li>
		    <li class="main-nav-search">
		      <a id="main-nav-search-link" class="search-link"></a>
		      <div id="main-nav-search-form" class="main-nav-search-form">
			<form role="search" id="searchform" method="get" action="http://intelliscence.com/web/work/demos/titan/">
			  <input type="text" value="" name="s" id="s" />
			  <input type="submit" value="&#xf002;" />
			</form>
		      </div>
		    </li>
		  </ul>
		</nav>
		<div class="mobile-nav-holder main-menu"></div>
	      </div>
	    </header>
	    </div>		<div class="init-sticky-header"></div>
	</div>
	<header id="header-sticky" class="sticky-header">
	  <div class="sticky-shadow">
	    <div class="avada-row">
	      <div class="logo">
		<a href="http://intelliscence.com/web/work/demos/titan">
		  <img src="http://intelliscence.com/web/work/demos/titan/wp-content/uploads/2014/04/main-logo3.png" alt="Titan" data-max-width="" class="normal_logo" />
		</a>
	      </div>
	      <nav id="sticky-nav" class="nav-holder">
		<ul class="navigation menu fusion-navbar-nav">
		  <li id="menu-item-4678" class="menu-item menu-item-type-custom menu-item-object-custom menu-item-4678"><a    href="#">Home</a></li>
		  <li id="menu-item-4707" class="menu-item menu-item-type-custom menu-item-object-custom menu-item-4707"><a    href="#">Learn</a></li>
		  <li id="menu-item-4708" class="menu-item menu-item-type-custom menu-item-object-custom menu-item-4708"><a    href="#">Showcase</a></li>
		  <li id="menu-item-4709" class="menu-item menu-item-type-custom menu-item-object-custom menu-item-4709"><a    href="#">Support</a></li>
		  <li id="menu-item-4710" class="menu-item menu-item-type-custom menu-item-object-custom menu-item-4710"><a    href="#">Download</a></li>
		  <li id="menu-item-4711" class="menu-item menu-item-type-post_type menu-item-object-page current-menu-item page_item page-item-3610 current_page_item menu-item-4711"><a    href="#">Documentation</a></li>
		  <li id="menu-item-4712" class="menu-item menu-item-type-custom menu-item-object-custom menu-item-4712"><a    href="#">Blog &#038; Events</a></li>
		  <li class="main-nav-search">
		    <a id="sticky-nav-search-link" class="search-link"></a>
		    <div id="sticky-nav-search-form" class="main-nav-search-form">
		      <form role="search" id="searchform" method="get" action="http://intelliscence.com/web/work/demos/titan/">
			<input type="text" value="" name="s" id="s" />
			<input type="submit" value="&#xf002;" />
		      </form>
		    </div>
		  </li>
		</ul>
	      </nav>
	      <div class="mobile-nav-holder"></div>
	    </div>
	  </div>
	</header>
	<div id="main" class="clearfix width-100" style="padding-left:20px;padding-right:20px">
          <xsl:apply-templates select="."/>
	</div>
	<div class="clearer"></div>
	<footer id="footer">
	  <div class="avada-row">
	    <ul class="copyright">
	      <li>Copyright 2014 All Rights Reserved - Titan <br />
	      Titan is a trademark of Aurelius LLC. Cassandra, HBase, and Hadoop are trademarks of the Apache Software Foundation.<br />
	      Documentation generated with <a href="http://www.methods.co.nz/asciidoc/">AsciiDoc</a>, <a href="http://asciidoctor.org/">AsciiDoctor</a>, <a href="http://docbook.sourceforge.net/">DocBook</a>, and <a href="http://saxon.sourceforge.net/">Saxon</a>.<br />
	      <span class='links'>Photo Attributions - Terms - Privacy</span></li>
	    </ul>
	  </div>
	</footer>
      </div>
      <script type='text/javascript' src='http://intelliscence.com/web/work/demos/titan/wp-content/themes/Avada/js/main.js'></script>
    </body>
  </xsl:template>
</xsl:stylesheet>