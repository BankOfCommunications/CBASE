#!/bin/sh
echo -e '#include "ob_item_type.h"'
echo -e "const char* get_type_name(int type)\n{"
echo -e "\tswitch(type){"
sed -rn 's/\s*(T_[_A-Z]+)\s*,/\tcase \1 : return \"\1\";/p' $1
echo -e '\tdefault:return "Unknown";\n\t}\n}'
