# xml-stax-mr
Xml StaX InputFormat and RecordReader for use with Large Un/Structured Xml Documents  

#### How to use Xml StaX InputFormat and RecordReader Sample Program  
/apps/xmlmr/xmlmr -xml_input_path ./wxdata -xml_output_path wxdata_output -xml_start_tag METAR -xml_elements METAR.temp_c=convert_f

#### Sample Program Options  
xml_input_path or xml_output_path or xml_start_tag  
usage: get [-help] [-xml_attributes <arg>] [-xml_elements <arg>] [-xml_input_path <arg>] [-xml_output_path <arg>] [-xml_start_tag <arg>]  
XmlMrDriver  
 -help                    Display help  
 -xml_attributes <arg>    XML Attributes to convert in bulk -METAR.temp_c=convert_f;METAR.dewpoint_c=convert_f  
 -xml_elements <arg>      Xml Elements to convert in bulk -METAR.temp_c=convert_f;METAR.dewpoint_c=convert_f  
 -xml_input_path <arg>    Input Folder/Path in HDFS of Xml files to be converted  
 -xml_output_path <arg>   Output Folder/Path in HDFS of Xml files to be converted  
 -xml_start_tag <arg>     Start Tag for the RecordReader - weather, METAR  
