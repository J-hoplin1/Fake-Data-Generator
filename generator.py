import asyncio,os,json,functools,time,sys
from faker import Faker
import pandas as pd
from typing import Any, MutableSequence
from datetime import datetime

class generator(object):
    __fake = Faker()
    __config = "config.json"
    __supportedExtension = ["csv","xlsx"]

    class WrongDataTypeException(Exception):
        def __init__(self):
            super().__init__("Wrong datatype detected")

    class NotSupportedExtensionException(Exception):
        def __init__(self):
            super().__init__("Not supported extension")

    def __init__(self):
        pass

    # Generator : If title need to be generate as random
    @classmethod
    def __titleRandomGenerator(cls,setCount):
        counter = 0
        while counter < setCount:
            t = cls.__fake.text(max_nb_chars=10)[:-1]
            yield t
            counter += 1

    # Generator : If title need to be generate as static
    @classmethod
    def __titleStaticGenerator(cls,title,setCount):
        counter = 1
        while counter <= setCount:
            yield f"{title}_{counter}"
            counter += 1

    @classmethod
    def __mapper(cls,field,rows) -> Any:
        type = field["type"]
        if  type == "datetime":
            return functools.partial(cls.__generateDateTime,rows)
        elif type == "int":
            min = (field["min"] if field["min"] >= 0 else 0) if "min" in field.keys() else 0
            max = (field["max"] if field["max"] <= 9999 else 9999) if "max" in field.keys() else 9999
            return functools.partial(cls.__generateInt,rows,min,max)
        elif type == "str":
            max_length = (field["max_length"] if field["max_length"] > 0 else 200) if "max_length" in field.keys() else 250
            return functools.partial(cls.__generateText,rows,max_length)
        else:
            raise cls.WrongDataTypeException

    @classmethod
    async def __generateDateTime(cls,counts:int) -> MutableSequence:
        return [ cls.__fake.date_time() for _ in range(counts)]

    @classmethod
    async def __generateText(cls,counts:int,max_nb_chars:int=15) -> MutableSequence:
        return [ cls.__fake.text(max_nb_chars=max_nb_chars) for _ in range(counts)]

    @classmethod
    async def __generateInt(cls,counts:int,min,max) -> MutableSequence:
        return [ cls.__fake.random_int(min=min,max=max) for _ in range(counts)]

    @classmethod
    async def __generation(cls,directory,title,extension,config,number):
        print(f"Working : Dataset No.{number}")
        rowCount = config["rowCount"] if config["rowCount"] or config["rowCount"] < 0 else 1
        fieldInfos = {i["name"]: await cls.__mapper(i, rowCount)() for i in config["fields"]}
        df = pd.DataFrame(fieldInfos)
        saveLocation = f"{directory}/{title}.{extension}"
        df.to_csv(saveLocation,encoding="utf-8-sig") if extension == "csv" else df.to_excel(saveLocation)
        print(f"Complete to save dataset : {title}.{extension}")

    @classmethod
    async def generate(cls,datasetCount:int=1) -> None:
        directory = f"{os.getcwd()}/{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}_Test_Datasets"
        os.mkdir(directory)
        with open(cls.__config,'r') as j:
            config = json.load(j)
        # Check extension input
        if config["extension"] not in cls.__supportedExtension:
            raise generator.NotSupportedExtensionException()
        extension = config["extension"]
        # Check title needs to be generate as random or static
        title = cls.__titleStaticGenerator(config["title"],datasetCount) if config["title"] else cls.__titleRandomGenerator(datasetCount)
        # Execute async call in once
        await asyncio.gather(*[cls.__generation(directory,next(title),extension,config,i + 1) for i in range(datasetCount)])



if __name__ == "__main__":
    datasetCount = int(sys.argv[1]) if sys.argv[1] else 1
    start = time.process_time()
    asyncio.run(generator.generate(datasetCount))
    end = time.process_time()
    print(f"Estimate : {round(float(end-start),2)} sec")