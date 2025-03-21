class Social:
    def __init__(self, user_email):
        self.user_email = user_email
        self.following = []
        self.followers = []

    def follow(self, target_email):
        if target_email not in self.following:
            self.following.append(target_email)

    def unfollow(self, target_email):
        if target_email in self.following:
            self.following.remove(target_email)

    def get_following(self):
        return self.following

    def get_followers(self):
        return self.followers

    def __str__(self):
        return f"User: {self.user_email}, Following: {len(self.following)}, Followers: {len(self.followers)}"
        