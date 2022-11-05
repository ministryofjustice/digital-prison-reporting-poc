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
	                    	<h2 style="text-align:center">Setup <strong>your organization</strong></h2>
	                    	<h4 style="text-align:center;color:#b7c4cd">It's simple and takes less than a minute</h4>
	                        <form role="form" action="/register/3" method="post" onsubmit="javascript: return valid.check(this);">
	                            <fieldset>
	                                <div class="form-group">
	                                    <input class="form-control" placeholder="Organization name" id="organization" name="organization" autofocus>
	                                </div>
	                                <div class="form-group">
                                            <select class="form-control" id="teamsize" name="teamsize">
                                            	<option value="0">Team Size</option>
                                                <option value="1">1</option>
                                                <option value="2">2-5</option>
                                                <option value="5">5-9</option>
                                                <option value="10">10-20</option>
                                                <option value="20">20+</option>
                                            </select>
                                    </div>
		                            <button type="submit" class="btn btn-lg btn-success btn-block">Next >></button>
	                                <div class="foot">
	                                	<br/>
	                                	<p class="text-center"><small style="color:#b7c4cd">By signing up, you agree to our Terms of Service and Privacy Policy, including use of cookies.</small></p>
		                            </div>
	                            </fieldset>
	                        </form>
	                    </div>
	                </div>
	            </div>
	        </div>
	    </div>
		
	
    </body>
</html>