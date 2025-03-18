class Person:
    def __init__(self, first, last, id):
        self.first = first
        self.last = last
        self.name = first + " " + last
        self.id = id

    def __str__(self):
        return self.name
    
    def get_id(self):
        return id
    
    def get_name(self):
        return self.name