<%@ page language="java" contentType="text/html; charset=US-ASCII"
    pageEncoding="US-ASCII"%>
<!DOCTYPE html>
<html>
<head>
<meta charset="US-ASCII">
<title>Project 1</title>
</head>
<body>

<h3>TEST: ${test}</h3>
<h3>Session Message: ${cookie_str}</h3>
<h3>Session Expiration Time: ${cookie_timeout}</h3>
<form action="HelloWorld" method="get">
	<input type="submit" name="act" value="Replace">
	<input type="text" name="message"><br>
	<input type="submit" name="act" value="Refresh"><br>
	<input type="submit" name="act" value="Logout">
</form>


</body>
</html>