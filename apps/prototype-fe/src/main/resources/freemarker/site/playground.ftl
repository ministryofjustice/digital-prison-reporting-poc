[#ftl]
<head>
	<script src="https://cdn.datatables.net/1.12.1/js/jquery.dataTables.min.js"></script>
	
	<script>
		var queries = {};
		[#list queries?keys as key]
		queries["${key}"] = "${queries[key]}";
		[/#list]
		
		$(document).ready(function() {
			$("#qMenu").click(function() {
				$("#queryName").val($(this).text().trim());
				$("#query").val(queries[$(this).text().trim()]);
			});
			//$("#table").DataTable({ sort: false});
		});
		
		function applyQuery() {
			var parms = { query: $("#query").val() };
			// apply query
			$.post("/app/query", parms, function(data) {
				var response = JSON.parse(data);
				console.log(response);
				makeTable(response, "table");
				//var table=$("#table").DataTable();
				//table
					//.clear()
					//.rows.add(response)
					//.draw();
			});
		}
		
		function makeTable(json, target) {
			var tbl_body = "";
			var tbl_head = "";
    		var odd_even = false;
			$.each(json, function() {
		        var tbl_row = "";
		        tbl_head = "";
		        $.each(this, function(k , v) {
		        	tbl_head +="<th scope='col'>"+k+"</th>";
		            tbl_row += "<td>"+v+"</td>";
		        });
		        tbl_body += "<tr class=\""+( odd_even ? "odd" : "even")+"\">"+tbl_row+"</tr>";
		        odd_even = !odd_even;               
		    });
		    
		    $("#" + target +" thead").html("<tr>"+tbl_head+"</tr>");
		    $("#" + target +" tbody").html(tbl_body);
		}
	</script>
</head>
<h1>Playground</h1>
<br/>
<br/>
[#-- SELECT of queries. All have a date range --]
<div class="row">
	<div class="dropdown">
	  <button class="btn btn-secondary dropdown-toggle" type="button" data-toggle="dropdown" aria-expanded="false">
	    Queries
	  </button>
	  <div id="qMenu" class="dropdown-menu">
	  	[#list queries?keys as key]
	    	<a id="${key}" class="dropdown-item" href="#">${key}</a>
	    [/#list]
	  </div>
	</div>
</div>
<br/>
<form id="form" action="#">
<div class="row">
	<textarea id="query" type="text" class="form-control" readonly>
	</textarea>
	<input id="queryName" type="hidden"></input>
</div>
</form>
<br/>
[#-- From Date /To Date --]
<div class="row">
</div>
[#--  Execute --]
<div class="row">
	<span class="pull-right"><a href="#" onclick="applyQuery(); return false;" class="btn btn-primary">Execute</a></span>
</div>
<br/>
<br/>
[#-- Table --]
<div class="row">
	<table id="table" class="table table-striped table-bordered" style="width:100%">
		<thead class="thead-dark"></thead>
		<tbody></tbody>
	</table>
</div>
