[#ftl]
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" lang="en" xml:lang="en">
	<head>
		<title>Register with Modular Data</title>
		<style>
			body {
				background-color: #FFFFFF !important;
			}
		</style>
	</head>
	<body>
	
	
		<div class="container">
	        <div class="row">
	            <div class="col-md-6 col-md-offset-4">
	                <div class="login-panel panel panel-default">
	                    <div class="panel-body">
	                    	<h2 style="text-align:center">Tell us <strong>about you</strong></h2>
	                    	<h4 style="text-align:center;color:#b7c4cd">It's simple and takes less than a minute</h4>
	                        <form role="form" action="/register/2" method="post" onsubmit="javascript: return valid.check(this);">
	                            <fieldset>
	                                <div class="form-group">
	                                    <input class="form-control" placeholder="Your full name" id="name" name="name" autofocus>
	                                </div>
	                                <div class="form-group">
	                                    <input class="form-control" placeholder="Your phone (complete with country code)" id="phone" name="phone">
	                                </div>
		                            <button type="submit" class="btn btn-lg btn-success btn-block">Next >></button>
	                            </fieldset>
	                        </form>
	                    </div>
	                </div>
	            </div>
	        </div>
	    </div>
		
	
    </body>
</html>