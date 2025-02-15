from __future__ import unicode_literals
import argparse
from subprocess import call
import pandas as pd
import os
from tqdm import tqdm
from joblib import Parallel, delayed


def downloader(row):
    link = "https://www.youtube.com/watch?v=" + row['video_link']
    print(link)
    if 'youtube' in link:
        try:
            output_dir = os.path.join(BASE_PATH, "video/raw_full")
            output_path = os.path.join(output_dir, f"{row['video_link'][-11:]}.mp4")
            print(f"&&&{row['video_link'][-11:]}")
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            # if os.path.exists(output_path):
            #     print(f"Find {output_path}, skip")
            # if not(os.path.exists(os.path.dirname(output_dir))):
            #     os.makedirs(os.path.dirname(output_dir))
            #download audio
            command = f'yt-dlp -f bestvideo+bestaudio --merge-output-format mp4 {link} -o {output_path}'
            print(f"Running command: {command}")
            res1 = call(command, shell=True)
            if res1 != 0:
                print(f"Download failed with code: {res1}")
                raise Exception(f"yt-dlp failed to download: {link}")
        except Exception as e:
            print("error, skip:", e)
    # else: #using youtube-dl to download jon
    #     output_path = os.path.join(BASE_PATH, "raw_full", row["speaker"],str(row["video_fn"])[:-4] + ".mkv")
    #     if os.path.exists(output_path):
    #         print(f"Find {output_path}, skip")
    #     print(link)
    #     command = 'youtube-dl -f bestvideo+bestaudio --merge-output-format mkv {link} -o "{output_path}"'.format(link=link, output_path=output_path)
    #     res1 = call(command, shell=True)

def save_interval(input_fn, start, end, output_fn):
    cmd = 'ffmpeg -i "%s"  -ss %s -to %s -strict -2 "%s" -y' % (
    input_fn, start, end, output_fn)
    res3 = call(cmd, shell=True)

def crop_tool(interval):
    try:
        # 解析开始和结束时间
        start_time = str(interval['start_time'])
        end_time = str(interval['end_time'])
        
        # 获取视频 ID
        video_id = interval['video_link'][-11:]
        input_fn = os.path.join(args.base_path, "video/raw_full", f"{video_id}.mp4")
        
        # 定义输出文件路径
        output_dir = os.path.join(args.base_path, "video/raw_cropped")
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        output_fn = os.path.join(output_dir, f"{start_time}_{end_time}.mp4")
        
        # 检查输入文件是否存在
        if not os.path.exists(input_fn):
            raise FileNotFoundError(f"Input file not found: {input_fn}")
        if os.path.exists(input_fn):
            print(f"正在裁剪: {input_fn} 从 {start_time} 到 {end_time}")
            save_interval(input_fn, start_time, end_time, output_fn)
        else:
            print(f"输入文件未找到，跳过: {input_fn}")
        
        # 裁剪视频
        save_interval(input_fn, start_time, end_time, output_fn)
    except Exception as e:
        print(f"Error: {e}")
        print(f"Couldn't crop interval: {interval}")


# def crop_tool(interval):
#     try:
#         start_time = str(pd.to_datetime(interval['start_time']).time())
#         end_time = str(pd.to_datetime(interval['end_time']).time())
#         video_id = (interval["video_link"][-11:])
#         OUTPUT_DIR  = os.path.join(args.base_path, "raw" , interval['speaker'] + "_cropped")
#         if not(os.path.isdir(OUTPUT_DIR)): os.makedirs(OUTPUT_DIR)
#         if (interval["speaker"] == 'jon') and ('youtube' not in interval["video_link"]):
#             video_id = (interval["video_fn"])[:-4]
#         input_fn = os.path.join(args.base_path,"raw_full", interval['speaker'], video_id + ".mkv")
#         output_fn = os.path.join(args.base_path, "raw" , interval['speaker'] + "_cropped", "%s.mp4"%(interval['interval_id'])) 
#         if not(os.path.exists(output_fn)):
#             save_interval(input_fn, str(start_time), str(end_time), output_fn)
#     except Exception as e:
#         print(e)
#         print("couldn't crop interval: %s"%interval)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-base_path', '--base_path', help='base folder path of dataset')
    parser.add_argument('-speaker', '--speaker', help='download videos of a specific speaker')
    parser.add_argument('-interval_path', '--interval_path', help='path to the intervals_df file')
    args = parser.parse_args()
    BASE_PATH = args.base_path #Path which to create raw folder and has intervals_df and video_links.csv

    df = pd.read_csv(os.path.join(BASE_PATH, args.interval_path))
    if args.speaker:
        df = df[df['speaker'] == args.speaker]
    df_download = df.drop_duplicates(subset=['video_link'])
    Parallel(n_jobs=2)(delayed(downloader)(row) for _, row in tqdm(df_download.iterrows(), total=df_download.shape[0]))
    Parallel(n_jobs=1)(delayed(crop_tool)(interval) for _, interval in tqdm(df.iterrows(), total=df.shape[0]))
    # command = "rm -r {}".format(os.path.join(BASE_PATH, "raw_full"))
    # res1 = call(command, shell=True)