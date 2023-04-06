xml=$(cat test1.xml)
size=$(echo -n "$xml" | wc -c)
content="$size\n$xml"
echo -e "$content" | nc -w 1 vcm-32266.vm.duke.edu 12345

xml=$(cat test2.xml)
size=$(echo -n "$xml" | wc -c)
content="$size\n$xml"
echo -e "$content" | nc -w 1 vcm-32266.vm.duke.edu 12345

xml=$(cat test4.xml)
size=$(echo -n "$xml" | wc -c)
content="$size\n$xml"
echo -e "$content" | nc -w 1 vcm-32266.vm.duke.edu 12345
