# queries for ingesting raw data from s3


def staging_events_copy(config):

    ARN = config.get("IAM_ROLE", "ARN")
    LOG_DATA = config.get("S3", "LOG_DATA")

    return f"""
        copy staging.s_events
        from {LOG_DATA}
        iam_role '{ARN}'
        json 'auto ignorecase'
    """

def staging_songs_copy(config):

    ARN = config.get("IAM_ROLE", "ARN")
    SONG_DATA = config.get("S3", "SONG_DATA")

    return f"""
        copy staging.s_songs
        from {SONG_DATA}
        iam_role '{ARN}'
        json 'auto ignorecase'
    """
