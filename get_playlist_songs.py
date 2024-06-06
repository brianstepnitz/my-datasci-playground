import yt_dlp

def get_playlist_songs(url):
    ydl_opts = {
        'quite': True,
        'extract_flat': True,
        'skip_download': True,
        'force_generic_extractor': True,
        }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info_dict = ydl.extract_info(url, download=False)
        entries = info_dict.get('entries', [])

        songs = []
        for entry in entries:
            artist = entry.get('channel', 'Unknown Artist')
            title = entry.get('title', 'Unknown Title')
            songs.append(f'{artist} - "{title}"')

    return songs

if __name__ == '__main__':
    for song in get_playlist_songs('https://music.youtube.com/playlist?list=PLr8Myoplu3o_Tm0HcOn2pnjUgzpNbQyjB&si=JtFWNmnd_KfuojVg'):
        print(song)
