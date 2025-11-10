import open3d as o3d
import numpy as np

# Шаг 1: Загрузка и визуализация
print("Шаг 1: Загрузка и визуализация")
mesh = o3d.io.read_triangle_mesh(o3d.data.BunnyMesh().path)
print(f"Количество вершин: {len(mesh.vertices)}")
print(f"Количество треугольников: {len(mesh.triangles)}")
print(f"Наличие цвета: {mesh.has_vertex_colors()}")
print(f"Наличие нормалей: {mesh.has_vertex_normals()}")
o3d.visualization.draw_geometries([mesh], window_name="Шаг 1: Исходная модель")

print("Понял: Загрузили 3D модель bunny, отобразили её. Модель имеет вершины, треугольники, но без цветов и нормалей по умолчанию.")

# Шаг 2: Преобразование в облако точек
print("\nШаг 2: Преобразование в облако точек")
pcd = mesh.sample_points_uniformly(number_of_points=10000)
print(f"Количество вершин: {len(pcd.points)}")
print(f"Наличие цвета: {pcd.has_colors()}")
o3d.visualization.draw_geometries([pcd], window_name="Шаг 2: Облако точек")

print("Понял: Преобразовали модель в облако точек, которое представляет поверхность набором точек. Цвета могут быть или нет.")

# Шаг 3: Реконструкция поверхности из облака точек
print("\nШаг 3: Реконструкция поверхности")
pcd.estimate_normals()
mesh_reconstructed, densities = o3d.geometry.TriangleMesh.create_from_point_cloud_poisson(pcd, depth=5)
# Удаление артефактов с помощью crop по bounding box
bbox = pcd.get_axis_aligned_bounding_box()
mesh_reconstructed = mesh_reconstructed.crop(bbox)
print(f"Количество вершин: {len(mesh_reconstructed.vertices)}")
print(f"Количество треугольников: {len(mesh_reconstructed.triangles)}")
print(f"Наличие цвета: {mesh_reconstructed.has_vertex_colors()}")
o3d.visualization.draw_geometries([mesh_reconstructed], window_name="Шаг 3: Реконструированная поверхность")

print("Понял: Из облака точек восстановили поверхность с помощью метода Пуассона, затем обрезали по bounding box для удаления артефактов.")

# Шаг 4: Вокселизация
print("\nШаг 4: Вокселизация")
voxel_grid = o3d.geometry.VoxelGrid.create_from_point_cloud(pcd, voxel_size=0.005)
print(f"Количество вокселей: {len(voxel_grid.get_voxels())}")  # Примерно
print(f"Наличие цвета: {False}")  # VoxelGrid не имеет цветов напрямую
o3d.visualization.draw_geometries([voxel_grid], window_name="Шаг 4: Воксельная сетка")

print("Понял: Преобразовали облако точек в воксельную сетку, где пространство разделено на кубики. Размер вокселя влияет на детализацию.")

# Шаг 5: Добавление плоскости
print("\nШаг 5: Добавление плоскости")
plane = o3d.geometry.TriangleMesh.create_box(width=0.01, height=0.2, depth=0.2)  # Тонкая плоскость
plane.translate([0, 0, 0])  # Расположить пересекая объект по центру
o3d.visualization.draw_geometries([mesh_reconstructed, plane], window_name="Шаг 5: Модель с плоскостью")

print("Понял: Добавили плоскость в сцену, пересекая объект по центру, для последующего клиппинга.")

# Шаг 6: Обрезка по поверхности
print("\nШаг 6: Обрезка по поверхности")
# Удалить точки справа от плоскости (x >= 0) для четкого клиппинга
points = np.asarray(mesh_reconstructed.vertices)
mask = points[:, 0] < 0  # Левая сторона от плоскости x=0
mesh_clipped = mesh_reconstructed.select_by_index(np.where(mask)[0])
print(f"Количество оставшихся вершин: {len(mesh_clipped.vertices)}")
print(f"Количество треугольников: {len(mesh_clipped.triangles)}")
print(f"Наличие цвета: {mesh_clipped.has_vertex_colors()}")
print(f"Наличие нормалей: {mesh_clipped.has_vertex_normals()}")
o3d.visualization.draw_geometries([mesh_clipped, plane], window_name="Шаг 6: После клиппинга")  # Показать с плоскостью для четкости

print("Понял: Удалили часть модели, которая находится по правую сторону от плоскости, оставив только левую часть. Плоскость пересекает объект.")

# Шаг 7: Работа с цветом и экстремумами
print("\nШаг 7: Работа с цветом и экстремумами")
# Убрать цвета и добавить градиент по X
mesh_clipped.vertex_colors = o3d.utility.Vector3dVector(np.zeros((len(mesh_clipped.vertices), 3)))
points = np.asarray(mesh_clipped.vertices)
min_x = np.min(points[:, 0])
max_x = np.max(points[:, 0])
colors = (points[:, 0] - min_x) / (max_x - min_x)
colors = np.column_stack([colors, np.zeros(len(colors)), 1 - colors])  # Градиент от красного к синему
mesh_clipped.vertex_colors = o3d.utility.Vector3dVector(colors)

# Найти экстремумы по X
min_point = points[np.argmin(points[:, 0])]
max_point = points[np.argmax(points[:, 0])]
print(f"Координаты минимума по X: {min_point}")
print(f"Координаты максимума по X: {max_point}")

# Выделить экстремумы сферой
sphere_min = o3d.geometry.TriangleMesh.create_sphere(radius=0.01)
sphere_min.translate(min_point)
sphere_max = o3d.geometry.TriangleMesh.create_sphere(radius=0.01)
sphere_max.translate(max_point)

o3d.visualization.draw_geometries([mesh_clipped, sphere_min, sphere_max], window_name="Шаг 7: С градиентом и экстремумами")

print("Понял: Убрали исходные цвета, применили градиент по оси X, нашли и выделили экстремальные точки.")