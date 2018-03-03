import io, json

def load_progress():
    default_status = {'current_index':0}
    try:
        with io.open('status.json', 'r') as status_file:
            status = json.load(status_file)
            status_file.close()
    except FileNotFoundError:
        #First time use
        status = default_status
        save_progress(status)
    return status
    
def save_progress(status):
    with io.open('status.json', 'w') as status_file:
        json.dump(status, status_file)
        status_file.close()

if __name__ == '__main__':
    s = load_progress()
    print(s)
    s['current_index'] = 10
    print(s)
    save_progress(s)
    s = load_progress()
    print(s)
