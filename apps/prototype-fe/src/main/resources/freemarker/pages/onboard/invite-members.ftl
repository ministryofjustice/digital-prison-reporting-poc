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
	                    	<h2 style="text-align:center">Invite your <strong>team members</strong></h2>
	                    	<h4 style="text-align:center;color:#b7c4cd">It's simple and takes less than a minute</h4>
	                        <form role="form" action="/register/4" method="post" onsubmit="javascript: return valid.check(this);">
	                            <fieldset>
	                            	<!-- for loop -->
	                                <div class="form-group input-group">
                                        <span class="input-group-addon"><i class="fa fa-envelope-o"></i>
                                        </span>
                                        <input type="text" class="form-control" placeholder="Team member email" id="teamEmail" name="teamEmail">
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