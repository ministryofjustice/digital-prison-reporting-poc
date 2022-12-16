[#ftl strip_whitespace=true]
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en" class="app">
    <head>
        
	    <meta charset="utf-8">
	    <meta name="viewport" content="width=device-width, initial-scale=1.0">
	    <!-- app.ftl -->
	
	    <title>${title}</title>
	    
	
	    <!--STYLESHEET-->
	    <!--=================================================-->
		<link href="https://fonts.googleapis.com/css?family=Lato|Yanone+Kaffeesatz|Open+Sans" rel="stylesheet">
	    <link href="/assets/css/bootstrap.min.css" rel="stylesheet">
	    <link href="/assets/css/nifty.min.css" rel="stylesheet">
	    <link href="/assets/css/nifty-icons.min.css" rel="stylesheet">
	    <link href="/assets/plugins/ionicons/css/ionicons.min.css" rel="stylesheet">
	    <link href="/assets/premium/icon-sets/icons/line-icons/premium-line-icons.min.css" rel="stylesheet">
	    <link href="/assets/premium/icon-sets/icons/solid-icons/premium-solid-icons.min.css" rel="stylesheet">
	    <link href="/assets/css/pace.min.css" rel="stylesheet">   
	    <link href="/assets/css/bootstrap-select-min.css" rel="stylesheet">    
		
	    <link href="/assets/plugins/font-awesome/css/font-awesome.min.css" rel="stylesheet">
	    <link href="/assets/plugins/themify-icons/themify-icons.min.css" rel="stylesheet">
	    <link href="/assets/plugins/switchery/switchery.min.css" rel="stylesheet">
	    <link href="/assets/css/app.css" rel="stylesheet">
	    
	    
	    <link href="/assets/css/custom-styles.css" rel="stylesheet">
	    
	    <style>
	    	.col-centered{
			    float: none;
			    margin: 0 auto;
			}
	    </style>
	
	    <!--JAVASCRIPT-->
	    <!--=================================================-->
	
	    <script src="/assets/js/pace.min.js"></script>
	    <script src="/assets/js/jquery.min.js"></script>
	    <script src="/assets/js/bootstrap.min.js"></script>
	    <script src="/assets/js/nifty.min.js"></script>  
	    <script src="/assets/js/custom-scripts.js"></script>
	    <script src="/assets/plugins/switchery/switchery.min.js"></script>
	    
	    ${head}
    </head>
    <body>
     	[#include "body-top.ftl"/]
     	[#include "content-top.ftl"/]
     	[#include "navbar.ftl" /]
        ${body}
     	[#include "content-tail.ftl"/]
    	[#include "body-tail.ftl"/]
        [#include "footer.ftl"/]
    </body>
</html>
