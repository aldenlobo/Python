#!/bin/bash
cd fusemount
mkdir dir1 dir12 dir13 dir14
echo "This is a test script." > test1.txt
ls
mkdir dir2 dir22 dir23
ls
cd dir2 		#In the 2nd level dir2.
ls
mkdir -p dir211/dir212/dir213/dir214
mkdir dir212
echo "This is a test script." > test2
ls				#Prints dir2 in the 2nd levels contents
cd ..			#Back to level 1 i.e. in fusemount
rmdir dir22 dir23
ls
cd dir2			#In level 2 i.e. dir2
ls
rmdir dir212
ls
cd ..			#Back to level 1 i.e. fusemount
ls -R
cp test1.txt car.txt
cat car.txt
mv test1.txt van.txt
ls
ln -s van.txt van_link.txt		#To create a link.
mv dir12 dir3
touch file1.txt
cat van.txt
ls
cd ..

