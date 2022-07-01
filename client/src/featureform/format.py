twoRowSpacing = "{:<30} {:<25}"
threeRowSpacing = "{:<30} {:<30} {:<30}"
fourRowSpacing = "{:<30} {:<30} {:<15} {:<30}"
divider = "-----------------------------------------------"

def formatRows(formatObj, formatObj2=None, formatObj3=None, formatObj4=None):
    if not formatObj2 and not formatObj3:
        for s in formatObj:
            if len(s) == 2:
                formatRows(s[0], s[1])
            elif len(s) == 3:
                formatRows(s[0], s[1], s[2])
            elif len(s) == 3:
                formatRows(s[0], s[1], s[2], s[3])
            else:
                return "Tuple length not formattable."
    elif formatObj2 and not formatObj3:
        print(twoRowSpacing.format(formatObj, formatObj2))
    elif formatObj2 and formatObj3 and not formatObj4:
        print(threeRowSpacing.format(formatObj, formatObj2, formatObj3))
    else:
        print(fourRowSpacing.format(formatObj, formatObj2, formatObj3, formatObj4))

def formatNewPara(s):
    print(divider)
    print(s)