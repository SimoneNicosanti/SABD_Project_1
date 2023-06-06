
def computePercentiles(x : tuple) -> tuple :
    valueList : list = x[1]
    
    listLen = len(valueList)

    median : float
    perc_25 : float
    perc_75 : float

    if (listLen % 2 == 0) :
        secondIndex = int(listLen / 2)
        firstIndex = secondIndex - 1

        secondValue = valueList[secondIndex]
        firstValue = valueList[firstIndex]
        
        median = (firstValue + secondValue) / 2
    
    else :
        medianIndex = int(listLen / 2)
        median = valueList[medianIndex]

    return (x[0], (perc_25, median, perc_75))
