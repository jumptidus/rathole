#!/bin/bash

# 设置目标架构
TARGETS=(
    "x86_64-unknown-linux-musl"
    # "aarch64-unknown-linux-musl" # 注释掉其他目标
    # "arm-unknown-linux-musleabihf"
    # "armv7-unknown-linux-musleabihf"
)

# 获取版本号
VERSION=$(grep '^version = ' Cargo.toml | cut -d '"' -f2)

# 设置输出目录
CURRENT_DATE=$(date +"%Y%m%d")
OUTPUT_DIR="releases/server_${CURRENT_DATE}_v${VERSION}"
mkdir -p $OUTPUT_DIR

# 安装必要的工具
brew install filosottile/musl-cross/musl-cross
brew install llvm
brew install arm-linux-gnueabihf-binutils

# 设置环境变量
export PATH="/opt/homebrew/opt/llvm/bin:$PATH"
export LDFLAGS="-L/opt/homebrew/opt/llvm/lib"
export CPPFLAGS="-I/opt/homebrew/opt/llvm/include"
export TARGET_CC=$(which clang)

export CC_x86_64_unknown_linux_musl="x86_64-linux-musl-gcc"
export AR_x86_64_unknown_linux_musl="x86_64-linux-musl-ar"

# 编译并复制每个目标 (现在只有一个目标)
for TARGET in "${TARGETS[@]}"
do
    echo "正在为 $TARGET 编译..."
    cargo build --release --target $TARGET

    if [ $? -eq 0 ]; then
        echo "编译成功,正在复制文件..."
        TARGET_DIR="$OUTPUT_DIR/$TARGET"
        mkdir -p $TARGET_DIR
        cp target/$TARGET/release/rathole $TARGET_DIR/
        echo "已复制 文件 到 $TARGET_DIR"
    else
        echo "编译 $TARGET 失败"
    fi
done

echo "所有目标编译完成"
