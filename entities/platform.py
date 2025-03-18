class Platform:
    def __init__(self, name, id):
        self.name = name
        self.id = id

    def __str__(self):
        return self.name
    
    def get_id(self):
        return self.id
    
    def get_name(self):
        return self.name