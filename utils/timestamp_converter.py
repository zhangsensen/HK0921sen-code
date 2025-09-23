
def convert_to_readable_timestamp(timestamp_data):
    """
    将各种格式的时间戳转换为可读格式

    Args:
        timestamp_data: 可以是多种格式的时间戳

    Returns:
        格式化后的时间字符串 "YYYY-MM-DD HH:MM:SS"
    """
    try:
        import pandas as pd

        if isinstance(timestamp_data, str):
            # 字符串格式
            if timestamp_data.isdigit():
                # 数字字符串
                if len(timestamp_data) == 13:  # 毫秒
                    dt = pd.to_datetime(int(timestamp_data), unit='ms')
                elif len(timestamp_data) == 10:  # 秒
                    dt = pd.to_datetime(int(timestamp_data), unit='s')
                else:
                    dt = pd.to_datetime(timestamp_data)
            else:
                # 可能是ISO格式或其他可读格式
                dt = pd.to_datetime(timestamp_data)

        elif isinstance(timestamp_data, (int, float)):
            # 数字时间戳
            if timestamp_data > 1e12:  # 毫秒
                dt = pd.to_datetime(timestamp_data, unit='ms')
            elif timestamp_data > 1e9:  # 秒
                dt = pd.to_datetime(timestamp_data, unit='s')
            else:
                dt = pd.to_datetime(timestamp_data)

        elif isinstance(timestamp_data, pd.Timestamp):
            # Pandas时间戳
            dt = timestamp_data

        else:
            # 其他格式
            dt = pd.to_datetime(timestamp_data)

        # 统一格式化
        return dt.strftime('%Y-%m-%d %H:%M:%S')

    except Exception as e:
        print(f"时间戳转换失败: {timestamp_data}, 错误: {e}")
        return str(timestamp_data)

# 使用示例
def format_dataframe_timestamps(df):
    """格式化DataFrame中的时间戳列"""
    if 'timestamp' in df.columns:
        df['timestamp'] = df['timestamp'].apply(convert_to_readable_timestamp)
    return df
