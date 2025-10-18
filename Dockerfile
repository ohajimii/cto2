# 用最新Deno官方镜像（1.46+，支持Oak）
FROM denoland/deno:1.46.3

# 设置工作目录
WORKDIR /app

# 复制所有文件（包括server.ts）
COPY . .

# 缓存依赖（Oak, djwt等，加速构建）
RUN deno cache server.ts

# 暴露端口（Render用10000）
EXPOSE 10000

# 启动命令（权限+跑server.ts）
CMD ["deno", "run", "--allow-net", "--allow-env", "server.ts"]
