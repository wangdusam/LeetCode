"""Module for pattern matching with regex"""
import re

# pylint: disable=C0103
# pylint: disable=C0301
# pylint: disable=W0612
# pylint: disable=R0912
# pylint: disable=R0914


def fn_getregex(pattern_type, case_type, string_type, fixedind):
    '''function for pattern matching with regex'''
    special_characters = "\/-+.@"
    pattern_val = pattern_type
    wordlenght = 0
    idx = 0
    filter_rexp = ""
    filter = ""
    subvalue = 0
    initfixedind = False
    initfixedind = fixedind
    date_afterdecimal=0
    date_decimallength = ""

    fixedind = True if string_type.lower() == "date" else fixedind

    fixedind = True if string_type.lower() == "date" else fixedind

    if len(re.findall("^(([-]|)[0-9]+$)", pattern_type)) != 0:
        filter = "^(([-]|)[0-9]+$)"
    elif len(re.findall("^([0-9a-zA-Z]+$)", pattern_type)) != 0:
        value_length = ""
        if fixedind == True:            
            value_length="{"+str(len(pattern_type))+"}"
        if string_type.lower() == "alpha":            
            if case_type.upper() == "UPPER" :                   
                filter = "^([A-Z]"+value_length+"+$)"
            elif case_type.upper() == "LOWER":
                filter = "^([a-z]"+value_length+"+$)" 
            else:
                filter = "^([a-zA-Z]"+value_length+"+$)" 
        if string_type.lower() == "numeric":
            filter = "^([0-9]"+value_length+"+$)"

        #filter = "^([0-9a-zA-Z]+$)"
    elif len(re.findall("^([0-9a-zA-Z]+$)", pattern_type)) != 0:
        filter = "^([0-9a-zA-Z]+$)"
    elif len(re.findall("^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.([a-zA-Z0-9-.]+$)", pattern_type)) != 0:
        
        filter = "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.([a-zA-Z0-9-.]+$)"
    elif len(re.findall("^[0-9][0-9][0-9][0-9]-((([0][0-1]|[0][3-9])|[1][0-2])-([0-2][0-9]|[3][0-1])|\
            (([0][2]|[0][3-9])|[1][0-2])-([0-2][0-9])) [0-5][0-9]:[0-5][0-9]:[0-5][0-9]((.([0-9]{1,6})( [a-zA-Z]{3}|))|( [a-zA-Z]{3}))$", pattern_type)) != 0:
        dec_val = 0
        date_decimallength = "((.([0-9]{1,6})( [a-zA-Z]{3}|))|( [a-zA-Z]{3}))" if initfixedind == False else  "( [a-zA-Z]{3})"
        if pattern_type.find(".")> 0:
            x_val = pattern_type[pattern_type.find(".")+1:]
            if x_val.rfind(" ")>0:
                x_val = x_val[:x_val.find(" ")]
            dec_val = len(x_val)
            date_decimallength = ".([0-9]{"+str(dec_val)+"})" + date_decimallength
            
        filter = "^[0-9][0-9][0-9][0-9]-((([0][0-1]|[0][3-9])|[1][0-2])-([0-2][0-9]|[3][0-1])|\
        (([0][2]|[0][3-9])|[1][0-2])-([0-2][0-9])) [0-5][0-9]:[0-5][0-9]:[0-5][0-9](("+date_decimallength+"))$"


    elif any(c in special_characters for c in pattern_type):
        
        # lists=list(pattern_type.lower().translate({ord(i): None for i in 'abcdefghijklmnopqrstuvwxyz1234567890'}))
        lists = list(re.sub(r'[!0-9a-zA-z]{0,100}', '', pattern_type.lower()))
        wordlenght = len(pattern_type)
        wordcount = 0
        i = 0
        ll = len(lists)
        #print(ll)
        value = "^"
        for l in lists:
            i = i + 1
            #print(i)
            wordcount = pattern_val.find(l)
            l = "\." if l == "." else l
            filter_rexp = pattern_val[idx:wordcount]
            pattern_val = pattern_val[wordcount + 1:wordlenght]
            #print(case_type.upper())
            #print(string_type)
            if filter_rexp.isdigit() == True or filter_rexp.upper() in ["YYYY", "YY", "MM", "DD", "HH", "SS"] or \
                    string_type == "numeric":
                value = value + "[0-9]" + ("{" + str(len(filter_rexp)) + "}" if fixedind == True else "+") + l
            elif string_type.lower() == "alpha":
                if case_type.upper() == "UPPER" :         
                    value = value + "[A-Z]" + ("{" + str(len(filter_rexp)) + "}" if fixedind == True else "+") + l
                elif case_type.upper() == "LOWER":
                    value = value + "[a-z]" + ("{" + str(len(filter_rexp)) + "}" if fixedind == True else "+") + l
                else:
                    value = value + "[A-Za-z]" + ("{" + str(len(filter_rexp)) + "}" if fixedind == True else "+") + l

            else:      
                         
                value = value + "[a-zA-Z0-9]" + ("{" + str(len(filter_rexp)) + "}" if fixedind == True else "+") + l
            #print(filter_rexp)
            #print(11)
            
            if i == ll:
                #print(filter_rexp)
                if filter_rexp.isdigit() == True or filter_rexp.upper() \
                        in ["SSSSSS", "SSSSS", "SSSS", "SSS", "SS", "S", "YYYY", "YY", "MM",
                            "DD"] or string_type == "numeric":
                    
                    if string_type.lower() == "date" and pattern_type[-4:].strip()=="UTC":
                        
                        value = value + "[a-zA-Z0-9]" + ("{" + str(len(pattern_val)) + "}" if fixedind == True else "+") + "$"
                    else:
                        
                        value = value + "[0-9]" + ("{" + str(len(pattern_val)) + "}" if fixedind == True else "+") + "$"
                elif string_type.lower() == "alpha":
                    if case_type.upper() == "UPPER":
                        value = value + "[A-Z]" + ("{" + str(len(pattern_val)) + "}" if fixedind == True else "+") + "$"
                    elif case_type.upper() == "LOWER":
                        value = value + "[a-z]" + ("{" + str(len(pattern_val)) + "}" if fixedind == True else "+") + "$"
                    else:
                        value = value + "[a-zA-z]" + ("{" + str(len(pattern_val)) + "}" if fixedind == True else "+") + "$"
                else:
                    value = value + "[a-zA-Z0-9]" + ("{" + str(len(pattern_val)) + "}"
                                                     if fixedind == True else "+") + "$"

                filter = value
    print(filter)
    return filter
    # print(filter)
