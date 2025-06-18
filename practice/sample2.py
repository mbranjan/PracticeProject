setTime = "07:30"
timeToSet = "08:00"

setHour = int(setTime.split(":")[0])
setMin = int(setTime.split(":")[1])

timeToSetHour = int(timeToSet.split(":")[0])
timeToSetMin = int(timeToSet.split(":")[1])

print(setMin, setHour)
print(timeToSetHour, timeToSetMin)
cnt = 0
hourDiff = abs(timeToSetHour - setHour)
minDiff = abs(timeToSetMin - setMin)

if hourDiff < 12:
    cnt = cnt + hourDiff
else:
    cnt = cnt + (24 - hourDiff)

if minDiff < 30:
    cnt = cnt + minDiff
else:
    cnt = cnt + (60 - minDiff)

print(cnt)