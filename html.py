def getHTMLReport(reportTitle, header_dict, details_dict, output_file_path):
    head = '''<html>

<head>
<script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
<script src="https://code.jquery.com/jquery-1.12.4.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/jspdf/1.3.3/jspdf.min.js"></script>
<script src="https://html2canvas.hertzen.com/dist/html2canvas.js"></script>

<script type="text/javascript">\n'''

    details = ''''''
    chart_call = ''''''
    body = '''<button onclick = "generate()" style="width: 150px; height: 30px; color: blue;"><b><u>Generate PDF</u></b></button>\n
<div id="content">
<h1 style="text-align: center; font-size: 32px; font-family: Verdana; color: #006600";><u>''' + reportTitle + '''</u></h1\n<br \><br \>\n\n'''
    
    for key in details_dict.keys():
       details = details + '''var ''' + key + '''_details = [ ''' + header_dict[key] + ''', ''' + details_dict[key] + ''' ]\n'''
       
       chart_call = chart_call + '''google.setOnLoadCallback(function(){drawChart("''' + key + '''", "pie''' + key + '''", "table''' + key + '''", ''' + key + '''_details);});\n'''
       
       body = body + '''<h1 style="text-align: center;">''' + key + ''' Summary</h1>
<table class="divtable">
<tr>
  <td>
     <div class="divstyle" id="table''' + key + '''"></div>
  </td>
  <td style="border-left: 100px solid #FFF;">
     <div class="divstyle" id="pie''' + key + '''"></div>
  </td>
</tr>
</table>\n
<br /><br />\n\n'''
       
    chart_load = '''\ngoogle.charts.load('current', {'packages':['corechart','table']});\n\n'''       

    chart_func_style = '''\nfunction drawChart(item, containerPie, containerTable, dataArray)
{
    var data = google.visualization.arrayToDataTable(dataArray, false);
	var containerPieDiv = document.getElementById(containerPie);
	var containerTableDiv = document.getElementById(containerTable);
	
	let pie = new google.visualization.PieChart(containerPieDiv);
	var pieOptions = {title: item, titleTextStyle: {color: '#006600', fontSize: 20}, is3D: true, backgroundColor: {fill:'#ffffe9', stroke:'red', strokeWidth:3}, chartArea:{width:'75%',height:'75%'}, legend:{alignment: 'center', textStyle:{bold: true, fontSize: 14}}, pieSliceText: 'value', pieSliceTextStyle:{fontSize: 15, bold: true}};
    
	let table = new google.visualization.Table(containerTableDiv);
	var tableOptions = {allowHtml: true, width:'100%', height:'100%', cssClassNames: { 
      headerRow: 'headerRow',
      tableRow: 'tableRow',
      oddTableRow: 'oddTableRow',
      selectedTableRow: 'selectedTableRow',
      headerCell: 'custom-table',
      tableCell: 'custom-table',
      rowNumberCell: 'rowNumberCell'
    }};
   	
    pie.draw(data, pieOptions);
	table.draw(data, tableOptions);
	
	google.visualization.events.addListener(table, 'sort',
        function(event)
	    {
           data.sort([{column: event.column, desc: !event.ascending}]);
           pie.draw(data, pieOptions);
        });
}

function generate() {
    var HTML_Width = $("#content").width();
    var HTML_Height = $("#content").height();
    var top_left_margin = 15;
    var PDF_Width = HTML_Width + (top_left_margin * 2);
    var PDF_Height = (PDF_Width * 1.5) + (top_left_margin * 2);
    var canvas_image_width = HTML_Width;
    var canvas_image_height = HTML_Height+400;

    var totalPDFPages = Math.ceil(HTML_Height / PDF_Height) - 1;

    html2canvas($("#content")[0]).then(function (canvas) {
        var imgData = canvas.toDataURL("image/jpeg", 1.0);
        var pdf = new jsPDF('p', 'pt', [PDF_Width, PDF_Height]);
        pdf.addImage(imgData, 'JPG', top_left_margin, top_left_margin, canvas_image_width, canvas_image_height);
        for (var i = 1; i <= totalPDFPages; i++) { 
            pdf.addPage(PDF_Width, PDF_Height);
            pdf.addImage(imgData, 'JPG', top_left_margin, -(PDF_Height*i)+(top_left_margin*4),canvas_image_width,canvas_image_height);
        }
        pdf.save("report.pdf");
    });
}
</script>

<style>
.divstyle {
  width: 450px;
  height: 300px;
}

.divtable {
  border:none;
  margin-left:auto;
  margin-right:auto;
}

table {
    border: 1px solid rgb(83, 133, 180);
}

.google-visualization-table-table th.custom-table {
    border: solid rgb(83, 133, 180);
	border-width: 1px 1px 1px 1px;
	font-size: 18px;
}

.google-visualization-table-table td.custom-table {
    border: solid rgb(83, 133, 180);
	border-width: 1px 1px 1px 1px;
	font-size: 18px;
}

.headerRow {
  text-align: center;
  background-color: rgb(83, 133, 180);
  border-color: rgb(151, 150, 168) !important;
  color: white;
  height: 50px;
}

.oddTableRow {
	background-color: rgb(232, 246, 247);
	text-align: center;
}

.tableRow { 
	background-color: rgb(246, 253, 253); 
	text-align: center;
} 

.tableRow:hover {background-color: rgba(233, 235, 154, 0.76) !important;}
.oddTableRow:hover {background-color: rgba(233, 235, 154, 0.76) !important;}

.selectedTableRow {
    background-color: rgba(141, 186, 238, 0.76) !important;
}
</style>
</head>

<body>\n\n'''

    body = body + '''</div>\n\n</body>\n\n</html>'''

    HTML = head + details + chart_load + chart_call + chart_func_style + body
    
    report = open(output_file_path, 'w')
    report.write(HTML)
    report.close()
