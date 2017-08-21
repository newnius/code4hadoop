#! /usr/bin/env python
import matplotlib.pyplot as plt

def load():
	x = []
	y = []
	colors = []
	size = []

	with open('trained_points.txt', 'r') as f:
		content = f.readlines()
	for line in content:
		id, cordinate, color = line.strip("\n").split("\t")
		i, j = cordinate.split(",")
		x.append(i)
		y.append(j)
		colors.append(int(color) * 0.3)
		size.append(50)

	with open('points.txt', 'r') as f:
		content = f.readlines()
	for line in content:
		id, cordinate = line.strip("\n").split("\t")
		i, j = cordinate.split(",")
		x.append(i)
		y.append(j)
		colors.append(1)
		size.append(100)
	plt.scatter(x, y, size, colors)
	plt.colorbar()
	plt.show()

if __name__ == '__main__':
	load()
