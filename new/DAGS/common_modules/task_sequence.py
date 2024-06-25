def task_seq(gscopr):
    details = dict({})
    old_val = old_intval = old_task_seq = ""
    del_task_seq = []
    old_val_index = ""
    for val in gscopr:
        str_val = str(val)
        task_nm = str_val[str_val.index(":") + 1:len(str_val) - 1]
        task_seq = task_nm[task_nm.rindex("_") + 1:].strip()
        details[task_seq] = {}
        aftr_dec_num = ("0" + task_seq[task_seq.find("."):])
        aftr_dec_val = task_seq[task_seq.find(".") + 1:]
        # print(task_seq)
        aftr_dec_val_2 = int(aftr_dec_val[:2])
        details[task_seq]["prv_val"] = old_val
        details[task_seq]["cur_val"] = str_val
        details[task_seq]["nxt_val"] = ""
        details[task_seq]["prv_index"] = old_val_index
        details[task_seq]["cur_index"] = gscopr.index(val)
        details[task_seq]["nxt_index"] = ""
        details[task_seq]["first_ind"] = False
        details[task_seq]["Last_ind"] = False
        details[task_seq]["multiple_ind"] = False

        if int(aftr_dec_val_2) > 0:
            # print(aftr_dec_val_2)
            if int(aftr_dec_val_2) == 99:
                del_task_seq.append(task_seq)

            if (aftr_dec_val_2 != old_intval):
                details[task_seq]["first_ind"] = True

                if old_task_seq != "":
                    details[old_task_seq]["Last_ind"] = True

            old_intval = aftr_dec_val_2
            old_task_seq = task_seq
        old_val = str_val
        old_val_index = gscopr.index(val)
    # print(details)
    for cal in details:
        task_seq = cal
        val = cal[:cal.index(".")]
        if details[task_seq]['first_ind'] == True:
            # print(val)
            details[task_seq]['prv_val'] = details[val + ".0"]['cur_val']
            details[task_seq]['prv_index'] = details[val + ".0"]['cur_index']
        if details[task_seq]['Last_ind'] == True:
            # print(val)
            for i in del_task_seq:
                if i[:i.index(".")] == val:
                    end_val = i
            details[task_seq]['nxt_val'] = details[end_val]['cur_val']
            details[task_seq]['nxt_index'] = details[end_val]['cur_index']

    del details['0.0']
    for task_seq in del_task_seq:
        del details[task_seq]

    for task_seq in details:
        if details[task_seq]['Last_ind'] == True:

            gscopr[details[task_seq]['prv_index']] >>  gscopr[details[task_seq]['cur_index']]  >> \
                  gscopr[details[task_seq]['nxt_index']]
        else:
            gscopr[details[task_seq]['prv_index']]  >>  gscopr[details[task_seq]['cur_index']]
    return "success"
    
    
    