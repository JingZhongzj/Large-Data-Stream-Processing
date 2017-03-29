import sys
import time
import matplotlib.pyplot as plt

orig_stdout = sys.stdout
f = open('/Users/zhongjing/Desktop/data.txt', 'w')
sys.stdout = f

data = []
k = 500
for i in range(1,k+1):
	tmp = [0]
	for j in range(1,i+1):
		tmp.append(j)
	print tmp
	data.append(tmp)

for i in range(1,k+1):
	tmp = [1]
	for j in range(1,i+1):
		tmp.append(j)
	print tmp
	data.append(tmp)

sys.stdout = orig_stdout
f.close()

tmp = data

def AB(k):
	start = time.time()
	# Operation of 'A', the selectivity is 1/2 since half of the lists that start with "1" are filtered out by filterA.
	a = filterA(tmp)
	# Operation of 'B', the selectivity is 1/k since there are only 1/k of the lists has length >= k.
	b = filterB(a, k) # k = 500 ~ 1, selectivity = 1/500 ~ 1
	end = time.time()
	return (end-start)

def BA(k):
	start = time.time()
	# Operation of 'B', the selectivity is 1/k since there are only 1/k of the lists has length >= k.
	b = filterB(tmp, k) # k = 500 ~ 1, selectivity = 1/500 ~ 1
	# Operation of 'A', the selectivity is 1/2 since half of the lists that start with "1" are filtered out by filterA.
	a = filterA(b)
	end = time.time()
	return (end-start)

def filterA(x):
	tmp = []
	for i in x:
		if i[0] == 0:
			tmp.append(i)
	return tmp

def filterB(x,k):
	tmp = []
	for i in x:
		if len(i) > k+1:
			tmp.append(i)
	return tmp

def res():
	arr = []
	for k in range(500, 0, -1):
		arr.append(BA(k)/AB(k))
	return arr

y1 = []
for i in range(1, 501):
	y1.append(1.0)

y2 = res()

x_axis = []
for i in range(500, 0, -1):
	x_axis.append(float(i)/500)


plt.plot(x_axis, y1, 'r--', x_axis, y2)
plt.axis([0.0,1.0,0.0,2.0])
plt.show()
