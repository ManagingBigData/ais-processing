# -*- coding: utf-8 -*-
"""
Created on Mon Dec  8 11:48:40 2014

@author: DobrkovicA
"""
import psycopg2

ROT_NOT_AVAILABLE_CONST = 999

sixBitASCIIdict = {'000000' : '@','000001' : 'A','000010' : 'B','000011' : 'C','000100' : 'D',
				   '000101' : 'E','000110' : 'F','000111' : 'G','001000' : 'H','001001' : 'I',
				   '001010' : 'J','001011' : 'K','001100' : 'L','001101' : 'M','001110' : 'N',
				   '001111' : 'O','010000' : 'P','010001' : 'Q','010010' : 'R','010011' : 'S',
				   '010100' : 'T','010101' : 'U','010110' : 'V','010111' : 'W','011000' : 'X',
				   '011001' : 'Y','011010' : 'Z','011011' : '[','011100' : '\\','011101' : ']',
				   '011110' : '\^','011111' : '_','100000' : ' ','100001' : '!','100010' : '"',
				   '100011' : '\#','100100' : '$','100101' : '%','100110' : '&','100111' : '\'',
				   '101000' : '(','101001' : ')','101010' : '\*','101011' : '\+','101100' : ',',
				   '101101' : '-','101110' : '.','101111' : '/','110000' : '0','110001' : '1',
				   '110010' : '2','110011' : '3','110100' : '4','110101' : '5','110110' : '6',
				   '110111' : '7','111000' : '8','111001' : '9','111010' : ':','111011' : ';',
				   '111100' : '<','111101' : '=','111110' : '>','111111' : '?'}

navigationStatus = {0: 'Under way using engine', 1: 'At anchor', 2: 'Not under command',
					3: 'Restricted manoeuverability', 4: 'Constrained by her draught', 5: 'Moored',
					6: 'Aground', 7: 'Engaged in Fishing', 8: 'Under way sailing',
					9: 'Reserved for future amendment of Navigational Status for HSC',
					10: 'Reserved for future amendment of Navigational Status for WIG',
					11: 'Reserved for future use', 12: 'Reserved for future use', 13: 'Reserved for future use',
					14: 'AIS-SART is active', 15: 'Not defined (default)'}

shipTypeDict = {0: 'Not available (default)', 1: 'Reserved for future use', 2: 'Reserved for future use',
				3: 'Reserved for future use', 4: 'Reserved for future use', 5: 'Reserved for future use',
				6: 'Reserved for future use', 7: 'Reserved for future use', 8: 'Reserved for future use',
				9: 'Reserved for future use', 10: 'Reserved for future use', 11: 'Reserved for future use',
				12: 'Reserved for future use', 13: 'Reserved for future use', 14: 'Reserved for future use',
				15: 'Reserved for future use', 16: 'Reserved for future use', 17: 'Reserved for future use',
				18: 'Reserved for future use', 19: 'Reserved for future use', 20: 'Wing in ground (WIG), all ships of this type',
				21: 'Wing in ground (WIG), Hazardous category A', 22: 'Wing in ground (WIG), Hazardous category B',
				23: 'Wing in ground (WIG), Hazardous category C', 24: 'Wing in ground (WIG), Hazardous category D',
				25: 'Wing in ground (WIG), Reserved for future use', 26: 'Wing in ground (WIG), Reserved for future use',
				27: 'Wing in ground (WIG), Reserved for future use', 28: 'Wing in ground (WIG), Reserved for future use',
				29: 'Wing in ground (WIG), Reserved for future use', 30: 'Fishing', 31: 'Towing',
				32: 'Towing: length exceeds 200m or breadth exceeds 25m', 33: 'Dredging or underwater ops',
				34: 'Diving ops' , 35 : 'Military ops' , 36 : 'Sailing' , 37 : 'Pleasure Craft',
				38: 'Reserved', 39: 'Reserved', 40: 'High speed craft (HSC)' , 41 : 'HSC: Hazardous cat A' ,
				42: 'HSC: Hazardous cat B' , 43: 'HSC: Hazardous cat C' , 44 : 'HSC: Hazardous cat D' ,
				45: 'High speed craft (HSC), Reserved for future use', 46: 'High speed craft (HSC), Reserved for future use',
				47: 'High speed craft (HSC), Reserved for future use', 48: 'High speed craft (HSC), Reserved for future use',
				49: 'HSC: No additional info' , 50: 'Pilot Vessel' , 51 : 'Search & Rescue vessel' ,
				52: 'Tug', 53 : 'Port Tender' , 54: 'Anti-pollution equipment' , 55 : 'Law Enforcement',
				56: 'Spare - Local Vessel', 57: 'Spare - Local Vessel', 58: 'Medical Transport',
				59: 'Noncombatant ship according to RR Resolution No. 18', 60 : 'Passenger' ,
				61: 'Passenger: Hazardous cat A' , 62 : 'Passenger: Hazardous cat B' ,
				63: 'Passenger: Hazardous cat C' , 64 : 'Passenger: Hazardous cat D' ,
				65: 'Passenger: Reserved for future use', 66: 'Passenger: Reserved for future use',
				67: 'Passenger: Reserved for future use', 68: 'Passenger: Reserved for future use',
				69: 'Passenger: No additional info' , 70 : 'Cargo' , 71 : 'Cargo: Hazardous cat A' ,
				72: 'Cargo: Hazardous cat B' , 73 : 'Cargo: Hazardous cat C' , 74 : 'Cargo: Hazardous cat D' ,
				75: 'Cargo: Reserved for future use', 76: 'Cargo: Reserved for future use',
				77: 'Cargo: Reserved for future use', 78: 'Cargo: Reserved for future use',
				79: 'Cargo: No additional info' , 80 : 'Tanker' , 81 : 'Tanker: Hazardous cat A' ,
				82: 'Tanker: Hazardous cat B' , 83 : 'Tanker: Hazardous cat C' , 84 : 'Tanker: Hazardous cat D' ,
				85: 'Tanker: Reserved for future use', 86: 'Tanker: Reserved for future use',
				87: 'Tanker: Reserved for future use', 88: 'Tanker: Reserved for future use',
				89: 'Tanker: No additional info' , 90 : 'Other Type' , 91 : 'Other Type: Hazardous cat A' ,
				92: 'Other Type: Hazardous cat B' , 93 : 'Other Type: Hazardous cat C' ,
				94: 'Other Type: Hazardous cat D' , 95: 'Other Type, Reserved for future use',
				96: 'Other Type, Reserved for future use', 97: 'Other Type, Reserved for future use',
				98: 'Other Type, Reserved for future use', 99: 'Other Type: no additional info'}

def int2bin(n, count=6):
	"""
	Get binary of integer value n, using count number of digits
	"""
	return ''.join([str((n >> y) & 1) for y in range(count-1, -1, -1)])

def bin2binstr(s):
	"""
	Helper function; joins bit array into one string
	"""
	return ''.join(s)

def bin2int(s, signed=False):
	"""
	Convert sequence of binary character into integer value
	Set signed value to true if input is signed value; default is False
	"""
	# check signed / unsigned flag
	if signed:
		# check the most significant bit to test negativity
		if s[0] == "1":
			inverse = ""
			# invert all bits
			for c in s[1:]:
				if c == "0":
					inverse += "1"
				else:
					inverse += "0"
			# convert to int and add 1
			val = int(inverse,2) + 1
			# return negative value
			return -val
		else:
			return int(s,2)
	else:
		return int(s,2)

def deg2degmin(Dd, crdnl):
	DEG = int(Dd)
	MINminFull = round((Dd - DEG) * 60, 6)
	return DEG, MINminFull, crdnl

def bin2Ascii(s):
	res = ""
	for i in range(len(s)//6):
		seg = s[i*6:i*6+6]
		if seg != "000000":
			res += sixBitASCIIdict[seg]
	return res

class Sentence:
	def __init__(self, coded_msg):
		self.decapsulate(coded_msg)

	def decapsulate(self, coded_msg):
		"""
		Accepts an NMEA 0183-formatted AIS message sentence,
		(i.e. a VDM encapsulated packet string).

		Returns the total count of sentence fragments of the arriving
		message; the present fragments's number; a sequential
		message ID for multi-sentence messages; a list of the
		significant binary digits in the sentence payload field;
		and boolean value if contained data passed validity test
		"""
		#cut of leading part of the message and trailing checksum
		start = coded_msg.find("!AIV")
		if start == -1:                         # not AIVDO, AIVDM message
			self.valid = False
			self.number_of_parts = -1
			#self.part_number = ""
			self.contained_parts = set()
			self.sequence_ID = ""
			self.raw = ""
			return self.number_of_parts, self.contained_parts, self.sequence_ID, self.raw, self.valid

		start += 7
		end = coded_msg.find(",0*")
		temp_raw = coded_msg[start:end]

		#extract segments
		segment_list = []
		segment_list = temp_raw.split(",")
		self.number_of_parts = int(segment_list[0]) # count of sentence parts

		self.contained_parts = set()
		self.contained_parts.add(int(segment_list[1]))   # 1 for single; 1, 2, ... for multi
		self.sequence_ID = segment_list[2]          # ID for sequential message
		if len(segment_list) >= 5:                  # ! some 3 sentence messages have empty raw segment
			self.raw = segment_list[4]              # raw message
			self.valid = True
		else:
			self.raw = ""
			self.valid = False

		return self.number_of_parts, self.contained_parts, self.sequence_ID, self.raw, self.valid

	def is_complete(self):
		"""
		Check if this message contains all parts
		"""
		if self.number_of_parts != len(self.contained_parts):
			return False
		else:
			res = True
			for el in range(1, self.number_of_parts + 1):
				if el not in self.contained_parts:
					res = False
			return res

	def merge(self, other_sentence):
		res = False
		if self.valid and other_sentence.valid:
			if self.sequence_ID == other_sentence.sequence_ID:
				self.contained_parts.update(other_sentence.contained_parts)
				self.raw += other_sentence.raw
				res = True
		return res

	def decode(self):
		"""
		Decodes AIS message and ...
		"""
		# get ascii value of every character and subtract 48
		# if the result is still higher than 40, subtract 8
		value_list = []
		for c in self.raw:
			val = ord(c) - 48
			if val > 40:
				val -= 8
			value_list.append(val)

		binary_list = []
		for i in value_list:
			binary_list.append(int2bin(i))

		# reassemble the sixbit strings
		self.bitstring = []
		for i in binary_list:
			lst = list(i)
			self.bitstring = self.bitstring + lst

		return self.bitstring

	def get_message_type(self):
		"""
		Returns message type of decoded message
		"""
		self.message_type = int(''.join(self.bitstring[:6]),2)
		return self.message_type


class NavigationBlock:
	def __init__(self, decoded_msg):
		self._01_MessageID = bin2binstr(decoded_msg[:6])        # 6        uint
		self._02_RepeatIndicator = bin2binstr(decoded_msg[6:8]) # 2        uint
		self._03_MMSI = bin2binstr(decoded_msg[8:38])           # 30       uint
		self._04_NavStatus = bin2binstr(decoded_msg[38:42])     # 4        uint
		self._05_ROT = bin2binstr(decoded_msg[42:50])           # 8  int Signed!
		self._06_SOG = bin2binstr(decoded_msg[50:60])           # 10    udecimal
		self._07_PositionAccuracy = bin2binstr(decoded_msg[61]) # 1         uint
		self._08_longitude = bin2binstr(decoded_msg[61:89])     # 28 decimal Signed
		self._09_latitude = bin2binstr(decoded_msg[89:116])     # 27 decimal Signed
		self._10_COG = bin2binstr(decoded_msg[116:128])         # 12    udecimal
		self._11_TrueHeading = bin2binstr(decoded_msg[128:137]) # 9        uint
		self._12_TimeStamp = bin2binstr(decoded_msg[137:143])   # 6        uint
		self._13_REST = bin2binstr(decoded_msg[143:])           # Drop these bits for now!

	def get_message_type(self):
		return int(self._01_MessageID, 2)

	def get_repeat_count(self):
		return int(self._02_RepeatIndicator, 2)

	def get_MMSI(self):
		return int(self._03_MMSI , 2)

	def get_navigation_status(self, verbal=False):
		if not verbal:
			return int(self._04_NavStatus ,2)
		else:
			return navigationStatus[int(self._04_NavStatus ,2)]

	# I DALJE BAGOVAN; pribaviti dobar uzorak i onda korigovati
	# implementirati posebne slucajeve prema dokumentaciji
	def get_ROT(self):
		val = bin2int(self._05_ROT, True)
		if val == 0:
			return 0, "C"
		elif val >= 0:                           # preserve sign
			if val == 127:
				return ROT_NOT_AVAILABLE_CONST, "R"
			elif val == 128:
				return ROT_NOT_AVAILABLE_CONST, "x"
			else:
				return (float(val) * 1000.0 / 4733) ** 2, "R"
		else:
			if val == -127:
				return -ROT_NOT_AVAILABLE_CONST, "L"
			elif val == -128:
				return -ROT_NOT_AVAILABLE_CONST, "x"
			else:
				return -(float(val) * 1000.0 / 4733) ** 2, "L"

	# note: value 102.2 indicates speeds of 102.2 knots or higher
	# note: value 102.3 indicates that speed is NOT AVAILABLE
	def get_SOG(self):
		return int(self._06_SOG, 2) / 10

	def get_longitude(self):
		# note: longitude is SIGNED binary
		val = bin2int(self._08_longitude,True)
		val /= 600000.0
		if val < 0:
			return round(val,6), "W"
		else:
			return round(val,6), "E"

	def get_longitude2(self):
		Dd, crdnl = self.get_longitude()
		return deg2degmin(Dd,crdnl)
		#return deg2degmin(self.get_longitude())

	def get_latitude(self):
		# note: longitude is SIGNED binary
		val = bin2int(self._09_latitude,True)
		val /= 600000.0
		if val < 0:
			return round(val, 6), "S"
		else:
			return round(val, 6), "N"

	def get_latitude2(self):
		Dd, crdnl = self.get_latitude()
		return deg2degmin(Dd,crdnl)

	def get_COG(self):
		return bin2int(self._10_COG) / 10

	def get_heading(self):
		return bin2int(self._11_TrueHeading)

	def get_timestamp(self):
		return bin2int(self._12_TimeStamp)


class TripBlock:
	def __init__(self, decoded_msg):
		self._01_MessageID = bin2binstr(decoded_msg[:6])            # 6        uint
		self._02_RepeatIndicator = bin2binstr(decoded_msg[6:8])     # 2        uint
		self._03_MMSI = bin2binstr(decoded_msg[8:38])               # 30       uint
		self._04_AISVersion = bin2binstr(decoded_msg[38:40])        # 2        uint
		self._05_IMO = bin2binstr(decoded_msg[40:70])               # 30       uint
		self._06_CallSign = bin2binstr(decoded_msg[70:112])         # 42       text
		self._07_ShipName = bin2binstr(decoded_msg[112:232])        # 120      text
		self._08_ShipType = bin2binstr(decoded_msg[232:240])        # 8        enum
		self._09_DimBow = bin2binstr(decoded_msg[240:249])          # 9        uint
		self._10_DimStern = bin2binstr(decoded_msg[249:258])        # 9        uint
		self._11_DimPort = bin2binstr(decoded_msg[258:264])         # 6        uint
		self._12_DimStarboard = bin2binstr(decoded_msg[264:270])    # 6        uint
		self._13_PositionFixType = bin2binstr(decoded_msg[270:274]) # 4        enum
		self._14_ETAMonth = bin2binstr(decoded_msg[274:278])        # 4        uint
		self._15_ETADay = bin2binstr(decoded_msg[278:283])          # 5        uint
		self._16_ETAHour = bin2binstr(decoded_msg[283:288])         # 5        uint
		self._17_ETAMinute = bin2binstr(decoded_msg[288:294])       # 6        uint
		self._18_Draught = bin2binstr(decoded_msg[294:302])         # 8        uint
		self._19_Destination = bin2binstr(decoded_msg[302:422])     # 8        text
		self._20_REST = bin2binstr(decoded_msg[422:])           # Drop these bits for now!

	def get_message_type(self):
		return int(self._01_MessageID, 2)

	def get_repeat_count(self):
		return int(self._02_RepeatIndicator, 2)

	def get_MMSI(self):
		return int(self._03_MMSI , 2)

	def get_AISversion(self):
		return int(self._04_AISVersion , 2)

	def get_IMO(self):
		return int(self._05_IMO , 2)

	def get_callsign(self, remove_quotes = False):
		res = bin2Ascii(self._06_CallSign)
		if remove_quotes:
			res = res.replace("'", "~")
			res = res.replace('"', '~')
		return res

	def get_shipname(self, remove_quotes = False):
		res = bin2Ascii(self._07_ShipName)
		if remove_quotes:
			res = res.replace("'", "~")
			res = res.replace('"', '~')
		return res

	def get_shiptype(self, verbal=False):
		if not verbal:
			return int(self._08_ShipType, 2)
		else:
			type_no = int(self._08_ShipType, 2)
			if type_no < 100:    # dictionary contains 100 types
				return shipTypeDict[type_no]
			else:
				return "Type not defined"

	def get_dimbow(self):
		return int(self._09_DimBow , 2)

	def get_dimstern(self):
		return int(self._10_DimStern , 2)

	def get_dimport(self):
		return int(self._11_DimPort , 2)

	def get_dimstarboard(self):
		return int(self._12_DimStarboard , 2)

	def get_length(self):
		return self.get_dimstern() + self.get_dimbow()

	def get_beam(self):
		return self.get_dimport() + self.get_dimstarboard()

	def get_PFT(self):
		# NOTE!: enumeration not yet implemented
		return int(self._13_PositionFixType, 2)

	def get_ETA_month(self):
		return int(self._14_ETAMonth, 2)

	def get_ETA_day(self):
		return int(self._15_ETADay, 2)

	def get_ETA_hour(self):
		return int(self._16_ETAHour, 2)

	def get_ETA_minute(self):
		return int(self._17_ETAMinute, 2)

	def get_draught(self):
		return int(self._18_Draught, 2) / 10

	def get_destination(self, remove_quotes = False):
		res = bin2Ascii(self._19_Destination)
		if remove_quotes:
			res = res.replace("'", "~")
			res = res.replace('"', '~')
		return res



class Decoder:
	def __init__(self):
		# Flags
		self.debug = False      # set True to output AIS messages in console

		# counters
		self.msg_read = 0
		self.msg_stored = 0
		self.report_freq = 5000  # default value for reporting on frequency of read messages

	def execute(self, it):

	  prev_msg = None
	  prev_ais = ""
	  prev_exists = False

	  for msg in it:
		  timestamp = msg[1]
		  cur_ais = msg[2]
		  cur_ais = cur_ais[0:-1]              # last character is line break so we want to exlude it
		  cur_msg = Sentence(cur_ais)

		  self.msg_read += 1                  # increment counter
		  if self.msg_read % self.report_freq == 0:     # display status
			  self.print_status()
		  try:
			# the "meat" of the method
			if cur_msg.is_complete():
				r = self.process_msg(timestamp, cur_msg)
				if r: yield r
				prev_exists = False
				prev_ais = ""
			else:
				if prev_exists:
					prev_msg.merge(cur_msg)
					prev_ais += "\n" + cur_ais
					if prev_msg.is_complete():
						r = self.process_msg(timestamp, prev_msg)
						if r: yield r
						prev_exists = False
					else:
						prev_exists = True
				else:
					prev_ais = cur_ais
					prev_msg = cur_msg
					prev_exists = True
		  except Exception,e:
			print e




	def process_msg(self, ts, msg):
		inserted = False
		dest_id = 0
		import datetime
		ts = int((ts - datetime.datetime(1970,1,1)).total_seconds())
		msg.decode()
		t = msg.get_message_type()
		# process message of type 5 - TripBlock
		if (t == 5):
			obj = TripBlock(msg.bitstring)
			result = {
			 'type': t,
			 'mmsi': obj.get_MMSI(),                 # mmsi
			 'imo': obj.get_IMO(),                  # IMO
			 'callsign': obj.get_callsign(True).strip(),     # callsign
			 'shipname': obj.get_shipname(True).strip(),     # ship name
			 'dimbow': obj.get_dimbow(),               # dimesnions bow
			 'dimstern': obj.get_dimstern(),             # dimensions stern
			 'dimport': obj.get_dimport(),              # dimenions port side
			 'dimstarboard': obj.get_dimstarboard(),         # dimensions starboard side
			 'eta_month': obj.get_ETA_month(),           # estimated time of arrival - month
			 'eta_day': obj.get_ETA_day(),             # estimated time of arrival - day
			 'eta_hour': obj.get_ETA_hour(),            # estimated time of arrival - hour
			 'eta_minute': obj.get_ETA_minute(),          # estimated time of arrival - minute
			 'draught': obj.get_draught(),             # draught
			 'destination': obj.get_destination(True).strip(),     # destination
			 'shiptype': obj.get_shiptype(),            # ship type (code)
			 'ts': ts,
			}
			return result


		# process message of type 1, 2, 3 - NavigationBlock
		elif ((t == 1) or (t == 2) or (t == 3)):
			obj = NavigationBlock(msg.bitstring)
			result = {
			  'type': t,
			  'mmsi': obj.get_MMSI(),                 #mmsi
			  'nav_status': obj.get_navigation_status(),    #nav_status
			  'rot_angle': obj.get_ROT()[0],               #rot_angle
			  'rot_direction': obj.get_ROT()[1],               #rot_direction
			  'sog': obj.get_SOG(),                  #sog
			  'cog': obj.get_COG(),                  #cog
			  'heading': obj.get_heading(),              #heading
			  'lat': obj.get_latitude()[0],          #lat
			  'lat2': obj.get_latitude()[1],         #lat_char
			  'lon': obj.get_longitude()[0],        #lon
			  'lon2': obj.get_longitude()[1],        #lon_char
			  'timestamp': obj.get_timestamp(),           #timestamp
			  'ts': ts
			}
			return result


	def print_status(self):
		print ("Messages read: ", self.msg_read, "Messages stored:", self.msg_stored)


"""
initiate decoder and run it for selected AIS messages
"""
import psycopg2
import psycopg2.extras
import json
import gzip
connection = psycopg2.connect("XXXX")
cursor = connection.cursor('cursor_unique_name', cursor_factory=psycopg2.extras.DictCursor)
cursor.execute("select * from raw_ais")
d = Decoder()

size = 0
out = None
i = 0
for d in d.execute(cursor):
  msg = json.dumps(d)
  size += len(msg)
  if out is None or size > int(5*1.5*128*1024*1024):
	if not out is None: out.close()
	out = gzip.open('ais/%04d.gz' % i, 'w')
	size = 0
	i += 1
  out.write(msg + '\n')
out.close()
