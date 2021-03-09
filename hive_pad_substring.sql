

#----attach 3 digit random number to a string & retrieve back

#---generate new number
SELECT concat('194564087231', lpad(floor(rand()*1000),3,"0"));
=>194564087231037

#---get the old number back
SELECT substr('194564087231037', 0,length("194564087231894")-3);
=>194564087231

