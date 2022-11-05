[#ftl strip_whitespace=true]
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en" class="app">
    <head>
        
	    <meta charset="utf-8">
	    <meta name="viewport" content="width=device-width, initial-scale=1.0">
	    <!-- theme/gds/default.ftl -->
	
	    <title>${title}</title>
	    
	
	    <!--STYLESHEET-->
	    <!--=================================================-->
		<link href="https://fonts.googleapis.com/css?family=Lato|Yanone+Kaffeesatz|Open+Sans" rel="stylesheet">
	    <link href="/assets/css/bootstrap.min.css" rel="stylesheet">
	    
	    <style>
	    	.col-centered{
			    float: none;
			    margin: 0 auto;
			}
	    </style>
	
	    <!--JAVASCRIPT-->
	    <!--=================================================-->
	
	    <script src="/assets/js/jquery.min.js"></script>
	    <script src="/assets/js/popper.min.js"></script>
	    <script src="/assets/js/bootstrap.min.js"></script>
	    <script src="/assets/js/bootstrap.bundle.min.js"></script>
		<script src="https://kit.fontawesome.com/7cce680f4c.js" crossorigin="anonymous"></script>
	    
	    ${head}
    </head>
    <body>
     	[#include "navbar.ftl" /]
     	[#include "body-top.ftl"/]
     	[#include "content-top.ftl"/]
        ${body}
     	[#include "content-tail.ftl"/]
    	[#include "body-tail.ftl"/]
    </body>
</html>
