[#ftl]
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" lang="en" xml:lang="en">
	<head>
		<title>Mockstar : Forgotten Password</title>
	</head>
	<body>
	
		<div class="container">
	        <div class="row">
	            <div class="col-md-5 col-md-offset-4">
		            <div class="login-panel panel panel-default">
		                    <div class="panel-heading">
		                        <h3 class="panel-title">Forgotten your password?</h3>
		                    </div>
		                    <div class="panel-body">
		                    	<p class="text-center">Enter your email address below and we'll send you instructions.</p>
			        			<form method="post" action="/password-retrieve" id="password-retrieve" onsubmit="javascript: return valid.check(this);" class="panel-body wrapper-lg"> 
						          <div class="form-group">
						            <label class="control-label">Your email</label>
						            <input name="email" value="" id="email" type="email" placeholder="test@example.com" class="form-control input-lg">
						          </div>
						        
						          <button type="submit" class="btn btn-lg btn-success btn-block">Recover Password</button>
						        </form>
						    </div>
						</div>
			     </div>
			 </div>
	    </div>   
    </body>
</html>