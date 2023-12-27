#pip install requests
import requests

'''
The download_file function downloads a file from a specified URL,
saves it with a given file name, and returns True if the download is successful 
(HTTP status code 200), otherwise prints an error message, returns False.
'''
def DownloadFile(url : str) -> str:
    response = requests.get(url)
    if response.status_code == 200:
        with open("imslasthour.xml", 'wb') as file:
            file.write(response.content)
        print(f"File imslasthour.xml downloaded successfully.")
        return "imslasthour.xml"

    print(f"Failed to download file. Status code: {response.status_code}")
    return None



if __name__ == "__main__":
    url = "https://ims.gov.il/sites/default/files/ims_data/xml_files/imslasthour.xml"

    DownloadFile(url)
