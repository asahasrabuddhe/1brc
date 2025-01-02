package onebrc

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func TestProcess(t *testing.T) {
	type args struct {
		path     string
		expected string
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "10k",
			args: args{
				path:     "/Users/ajitem/1brc/measurements.txt",
				expected: "{Abha=-29.0/18.4/62.7, Abidjan=-16.7/26.6/74.1, Abéché=-15.7/30.1/76.1, Accra=-22.4/27.0/73.3, Addis Ababa=-30.5/16.3/59.7, Adelaide=-30.7/17.7/60.5, Aden=-18.0/29.8/77.8, Ahvaz=-22.7/26.0/71.5, Albuquerque=-31.5/14.3/62.1, Alexandra=-32.8/11.2/64.0, Alexandria=-23.8/20.4/66.2, Algiers=-24.6/18.6/61.2, Alice Springs=-27.2/21.5/64.4, Almaty=-34.4/10.2/56.4, Amsterdam=-36.9/10.4/55.7, Anadyr=-53.3/-7.0/39.3, Anchorage=-40.4/2.8/47.6, Andorra la Vella=-41.1/9.9/58.1, Ankara=-36.6/12.2/59.4, Antananarivo=-29.1/18.3/62.4, Antsiranana=-22.5/25.8/71.4, Arkhangelsk=-42.4/1.3/45.2, Ashgabat=-29.9/17.4/65.2, Asmara=-29.3/15.9/60.5, Assab=-15.6/31.2/73.4, Astana=-44.4/3.6/49.6, Athens=-25.0/19.6/64.7, Atlanta=-27.1/17.4/62.9, Auckland=-32.1/15.6/63.3, Austin=-24.7/21.2/65.4, Baghdad=-20.3/23.3/70.4, Baguio=-24.7/19.9/62.7, Baku=-28.5/15.4/62.8, Baltimore=-31.6/13.4/58.9, Bamako=-20.6/28.5/72.5, Bangkok=-15.2/29.2/75.2, Bangui=-19.0/26.6/70.4, Banjul=-17.7/26.6/70.0, Barcelona=-34.5/18.6/64.8, Bata=-20.1/25.7/70.2, Batumi=-32.7/14.3/56.8, Beijing=-32.4/13.2/56.8, Beirut=-24.8/21.4/71.5, Belgrade=-38.5/12.8/54.4, Belize City=-16.2/27.3/71.2, Benghazi=-24.5/20.3/65.3, Bergen=-45.8/7.8/53.6, Berlin=-32.3/10.5/55.9, Bilbao=-29.7/15.0/58.2, Birao=-15.3/27.1/71.5, Bishkek=-32.3/11.5/56.1, Bissau=-17.0/27.7/70.9, Blantyre=-22.2/22.7/65.6, Bloemfontein=-30.5/15.9/61.7, Boise=-34.6/11.6/56.0, Bordeaux=-32.7/14.5/63.4, Bosaso=-15.8/30.7/76.0, Boston=-32.8/11.1/56.5, Bouaké=-25.7/26.6/70.5, Bratislava=-30.8/10.7/57.9, Brazzaville=-18.5/25.6/77.5, Bridgetown=-22.2/27.6/73.2, Brisbane=-21.1/21.9/67.8, Brussels=-37.6/10.7/56.7, Bucharest=-31.8/11.0/54.5, Budapest=-33.8/11.5/62.3, Bujumbura=-26.3/24.3/70.2, Bulawayo=-25.2/19.3/70.2, Burnie=-34.2/13.4/55.4, Busan=-29.5/15.3/61.5, Cabo San Lucas=-20.9/24.4/70.7, Cairns=-20.1/25.6/74.6, Cairo=-25.2/21.8/71.1, Calgary=-40.8/4.5/56.6, Canberra=-35.2/13.4/61.6, Cape Town=-28.9/16.5/66.9, Changsha=-29.6/17.8/60.9, Charlotte=-29.6/16.5/63.8, Chiang Mai=-18.3/26.4/74.7, Chicago=-34.1/10.0/55.0, Chihuahua=-26.6/19.0/67.7, Chittagong=-20.1/26.5/72.6, Chișinău=-31.8/10.4/57.1, Chongqing=-28.1/19.0/61.0, Christchurch=-31.1/12.5/56.1, City of San Marino=-31.1/12.0/55.9, Colombo=-20.9/28.1/73.6, Columbus=-37.7/11.9/60.2, Conakry=-16.0/27.0/71.9, Copenhagen=-37.9/9.3/53.4, Cotonou=-18.3/27.8/75.1, Cracow=-37.7/9.5/50.1, Da Lat=-31.1/18.3/60.1, Da Nang=-23.0/26.4/69.0, Dakar=-22.3/24.5/73.3, Dallas=-24.3/19.5/67.7, Damascus=-28.1/17.4/68.6, Dampier=-22.3/27.0/68.7, Dar es Salaam=-20.2/26.4/72.1, Darwin=-16.1/28.3/79.5, Denpasar=-19.5/24.2/70.2, Denver=-42.7/10.6/55.9, Detroit=-32.2/10.2/52.4, Dhaka=-26.5/26.5/70.9, Dikson=-61.8/-11.3/33.2, Dili=-19.4/27.2/71.2, Djibouti=-16.1/30.6/73.9, Dodoma=-20.7/23.2/66.3, Dolisie=-25.4/24.5/72.0, Douala=-19.3/27.3/76.5, Dubai=-25.4/27.5/70.9, Dublin=-38.8/10.0/58.7, Dunedin=-32.8/11.3/54.1, Durban=-22.4/21.1/67.3, Dushanbe=-31.8/15.0/58.9, Edinburgh=-34.5/9.5/56.6, Edmonton=-41.5/4.3/45.9, El Paso=-25.5/18.5/64.8, Entebbe=-21.7/21.5/63.4, Erbil=-24.4/19.9/68.7, Erzurum=-43.1/5.2/48.5, Fairbanks=-52.3/-2.3/50.9, Fianarantsoa=-27.6/18.3/62.9, Flores,  Petén=-17.8/27.0/70.6, Frankfurt=-32.0/10.8/56.1, Fresno=-37.4/18.3/62.8, Fukuoka=-27.5/17.4/67.6, Gaborone=-22.1/21.5/65.2, Gabès=-24.5/19.9/64.0, Gagnoa=-21.2/26.6/70.6, Gangtok=-29.4/15.5/60.2, Garissa=-20.1/30.0/74.2, Garoua=-18.2/29.0/70.9, George Town=-15.8/28.6/69.2, Ghanzi=-22.0/21.9/64.9, Gjoa Haven=-56.3/-14.7/26.1, Guadalajara=-25.1/21.3/64.7, Guangzhou=-28.3/22.9/66.1, Guatemala City=-24.2/20.9/62.8, Halifax=-34.9/7.6/53.7, Hamburg=-36.1/9.9/55.6, Hamilton=-29.8/14.1/59.3, Hanga Roa=-25.7/21.0/66.3, Hanoi=-21.1/24.1/74.7, Harare=-30.3/18.8/62.9, Harbin=-41.3/5.1/49.9, Hargeisa=-26.1/22.2/64.8, Hat Yai=-16.6/27.6/76.4, Havana=-18.4/25.8/74.9, Helsinki=-37.4/6.0/50.3, Heraklion=-28.5/19.3/64.0, Hiroshima=-27.4/16.6/60.8, Ho Chi Minh City=-18.1/28.0/74.1, Hobart=-33.1/13.0/58.4, Hong Kong=-23.2/23.8/70.2, Honiara=-20.6/27.1/77.0, Honolulu=-18.5/26.0/73.2, Houston=-25.9/21.3/64.9, Ifrane=-30.2/11.6/64.3, Indianapolis=-35.7/12.0/63.1, Iqaluit=-52.4/-9.5/36.1, Irkutsk=-43.4/1.0/44.1, Istanbul=-29.6/14.2/55.7, Jacksonville=-28.9/20.7/64.7, Jakarta=-19.1/27.3/70.4, Jayapura=-20.1/27.6/72.9, Jerusalem=-25.5/18.7/61.8, Johannesburg=-26.0/15.8/62.0, Jos=-22.8/23.3/66.9, Juba=-15.5/28.4/73.5, Kabul=-29.9/12.3/52.3, Kampala=-30.8/20.4/68.8, Kandi=-15.3/28.4/75.8, Kankan=-19.7/27.1/70.1, Kano=-25.2/27.0/69.4, Kansas City=-31.1/12.8/56.2, Karachi=-16.4/26.6/84.3, Karonga=-18.2/24.9/67.7, Kathmandu=-24.9/18.7/59.9, Khartoum=-19.0/30.6/74.7, Kingston=-29.1/28.1/78.3, Kinshasa=-18.0/25.9/72.4, Kolkata=-18.6/27.3/69.8, Kuala Lumpur=-15.8/27.9/69.9, Kumasi=-17.6/26.6/71.9, Kunming=-32.2/16.0/66.0, Kuopio=-47.2/3.5/52.8, Kuwait City=-21.0/26.3/72.8, Kyiv=-42.4/8.6/54.8, Kyoto=-33.8/16.1/59.6, La Ceiba=-21.0/26.8/69.4, La Paz=-22.6/24.3/71.1, Lagos=-18.4/27.4/70.7, Lahore=-18.2/24.9/68.1, Lake Havasu City=-23.3/24.3/75.0, Lake Tekapo=-32.5/8.9/53.1, Las Palmas de Gran Canaria=-20.8/21.7/66.5, Las Vegas=-28.2/20.7/62.6, Launceston=-31.7/13.4/58.1, Lhasa=-35.6/7.8/52.0, Libreville=-24.3/26.5/72.1, Lisbon=-26.7/17.9/59.4, Livingstone=-23.7/22.3/68.2, Ljubljana=-32.4/11.1/56.3, Lodwar=-17.7/30.0/75.8, Lomé=-18.1/27.5/76.7, London=-37.3/11.6/55.7, Los Angeles=-25.6/19.0/65.1, Louisville=-31.5/14.2/62.0, Luanda=-18.2/26.4/69.6, Lubumbashi=-21.7/21.3/65.2, Lusaka=-23.4/20.4/69.8, Luxembourg City=-35.3/9.5/58.2, Lviv=-38.9/7.9/49.9, Lyon=-32.3/12.7/57.5, Madrid=-25.2/15.3/59.7, Mahajanga=-21.1/26.9/71.1, Makassar=-23.8/27.3/70.1, Makurdi=-17.7/26.6/71.1, Malabo=-24.7/26.9/70.4, Malé=-14.7/28.6/74.2, Managua=-17.4/27.9/72.2, Manama=-20.0/27.1/72.3, Mandalay=-16.6/28.6/72.1, Mango=-20.2/28.7/72.6, Manila=-14.4/29.0/70.7, Maputo=-18.8/23.3/63.0, Marrakesh=-26.9/20.1/66.0, Marseille=-33.4/16.1/65.1, Maun=-19.8/22.9/64.0, Medan=-19.8/27.1/70.6, Mek'ele=-22.1/23.2/68.4, Melbourne=-32.3/15.4/60.1, Memphis=-27.5/17.6/60.3, Mexicali=-24.7/23.6/67.2, Mexico City=-25.0/17.9/62.9, Miami=-22.6/25.5/66.5, Milan=-30.6/13.2/57.5, Milwaukee=-33.3/9.1/53.1, Minneapolis=-34.9/8.0/52.9, Minsk=-42.8/6.8/48.7, Mogadishu=-16.5/27.7/74.2, Mombasa=-17.5/27.0/70.4, Monaco=-26.6/16.8/64.8, Moncton=-39.0/6.2/52.4, Monterrey=-23.9/22.8/65.6, Montreal=-35.7/6.9/50.9, Moscow=-42.0/5.9/51.7, Mumbai=-19.2/27.7/68.0, Murmansk=-41.3/0.6/44.4, Muscat=-15.4/28.7/75.1, Mzuzu=-32.2/18.1/61.5, N'Djamena=-14.4/29.0/76.1, Naha=-24.9/23.6/69.2, Nairobi=-27.7/18.2/65.3, Nakhon Ratchasima=-14.3/27.9/71.8, Napier=-28.7/14.9/58.5, Napoli=-28.6/16.3/63.8, Nashville=-28.4/15.7/62.5, Nassau=-20.4/25.2/67.6, Ndola=-24.9/20.8/68.9, New Delhi=-21.4/25.6/69.4, New Orleans=-26.3/21.1/64.2, New York City=-31.1/13.2/57.0, Ngaoundéré=-24.4/22.5/65.1, Niamey=-16.1/30.0/73.3, Nicosia=-26.6/20.1/63.8, Niigata=-33.7/14.2/58.0, Nouadhibou=-25.6/21.8/65.8, Nouakchott=-21.5/26.3/70.8, Novosibirsk=-50.1/1.7/51.5, Nuuk=-43.9/-1.4/44.8, Odesa=-40.2/10.9/54.3, Odienné=-20.4/26.6/70.6, Oklahoma City=-25.6/16.2/61.3, Omaha=-37.7/10.8/57.8, Oranjestad=-16.6/28.8/73.3, Oslo=-35.6/5.8/53.2, Ottawa=-38.0/6.7/53.7, Ouagadougou=-18.2/28.9/73.4, Ouahigouya=-19.8/29.2/73.8, Ouarzazate=-27.6/19.3/62.2, Oulu=-42.3/2.7/49.4, Palembang=-19.3/27.9/75.2, Palermo=-27.0/18.9/60.3, Palm Springs=-21.8/25.1/67.7, Palmerston North=-32.3/13.5/60.4, Panama City=-14.3/28.7/71.4, Parakou=-16.8/27.4/70.7, Paris=-36.6/12.5/56.3, Perth=-27.9/19.1/60.3, Petropavlovsk-Kamchatsky=-42.9/1.9/46.1, Philadelphia=-29.8/13.5/59.4, Phnom Penh=-16.2/29.0/81.6, Phoenix=-29.2/24.5/68.8, Pittsburgh=-34.9/11.0/52.2, Podgorica=-32.2/15.6/60.2, Pointe-Noire=-19.5/26.7/72.8, Pontianak=-15.3/28.3/74.6, Port Moresby=-16.5/27.5/71.2, Port Sudan=-15.1/29.1/76.6, Port Vila=-23.6/24.9/66.2, Port-Gentil=-25.8/26.6/70.7, Portland (OR)=-31.2/12.6/57.7, Porto=-30.9/16.0/61.3, Prague=-35.8/8.6/52.0, Praia=-19.5/24.9/69.6, Pretoria=-28.0/18.6/65.2, Pyongyang=-36.7/11.0/55.0, Rabat=-30.2/17.5/60.6, Rangpur=-17.9/25.0/70.6, Reggane=-19.6/28.9/73.4, Reykjavík=-38.0/4.3/49.3, Riga=-39.0/6.3/49.4, Riyadh=-17.0/26.6/76.1, Rome=-30.3/15.5/60.2, Roseau=-17.4/26.8/71.6, Rostov-on-Don=-35.4/10.1/54.1, Sacramento=-32.0/16.7/61.0, Saint Petersburg=-36.9/5.9/47.7, Saint-Pierre=-41.5/5.8/50.5, Salt Lake City=-34.6/11.9/58.1, San Antonio=-21.9/21.3/68.4, San Diego=-25.5/18.2/70.8, San Francisco=-27.7/14.9/68.7, San Jose=-27.2/16.8/60.8, San José=-22.6/23.1/69.3, San Juan=-16.1/27.8/75.2, San Salvador=-24.8/23.6/70.4, Sana'a=-25.1/20.4/66.2, Santo Domingo=-20.4/26.5/69.6, Sapporo=-36.0/9.1/53.6, Sarajevo=-35.8/10.3/53.3, Saskatoon=-51.9/3.3/52.7, Seattle=-35.1/11.5/57.0, Seoul=-40.3/12.8/56.1, Seville=-25.4/19.6/63.8, Shanghai=-27.7/17.1/60.2, Singapore=-19.0/27.6/73.9, Skopje=-31.2/12.6/56.5, Sochi=-32.4/14.5/62.3, Sofia=-36.5/10.8/54.1, Sokoto=-23.6/28.6/72.9, Split=-26.7/16.5/62.2, St. John's=-46.7/5.1/52.3, St. Louis=-39.5/14.2/57.0, Stockholm=-39.0/6.7/50.1, Surabaya=-16.8/27.8/70.5, Suva=-18.8/26.2/71.9, Suwałki=-38.5/7.3/57.5, Sydney=-23.0/18.0/59.8, Ségou=-14.2/28.7/71.2, Tabora=-23.5/23.5/69.7, Tabriz=-33.5/12.9/62.1, Taipei=-23.7/23.5/69.8, Tallinn=-37.1/6.5/51.4, Tamale=-16.4/28.6/74.0, Tamanrasset=-26.8/22.2/62.3, Tampa=-22.6/23.4/64.0, Tashkent=-29.7/15.1/59.3, Tauranga=-29.6/15.1/59.9, Tbilisi=-38.4/13.2/60.4, Tegucigalpa=-22.0/22.2/67.0, Tehran=-32.3/17.4/64.3, Tel Aviv=-22.4/20.5/64.8, Thessaloniki=-28.8/16.3/63.3, Thiès=-23.1/24.5/68.4, Tijuana=-25.7/18.2/63.0, Timbuktu=-15.3/28.7/71.3, Tirana=-30.5/15.5/63.0, Toamasina=-23.1/23.9/72.2, Tokyo=-29.0/15.7/63.1, Toliara=-18.0/24.6/68.1, Toluca=-31.0/12.7/62.9, Toronto=-40.1/9.6/54.2, Tripoli=-23.2/20.4/64.5, Tromsø=-42.0/2.9/53.1, Tucson=-22.3/21.4/64.0, Tunis=-29.1/18.8/68.8, Ulaanbaatar=-46.4/-0.4/42.7, Upington=-19.6/20.9/64.6, Vaduz=-33.3/10.3/54.1, Valencia=-28.7/18.7/64.1, Valletta=-26.4/19.2/64.0, Vancouver=-34.3/10.6/57.2, Veracruz=-18.5/26.0/74.5, Vienna=-34.4/10.6/55.4, Vientiane=-19.0/26.5/71.2, Villahermosa=-17.1/27.7/71.5, Vilnius=-37.1/6.1/53.3, Virginia Beach=-29.2/16.1/61.4, Vladivostok=-40.9/5.0/47.1, Warsaw=-41.9/8.7/53.9, Washington, D.C.=-30.5/14.9/62.6, Wau=-15.7/28.4/75.9, Wellington=-35.1/13.1/59.7, Whitehorse=-43.8/0.0/49.4, Wichita=-30.3/14.2/58.2, Willemstad=-20.0/28.7/70.9, Winnipeg=-39.1/3.0/55.8, Wrocław=-36.1/9.8/56.7, Xi'an=-30.0/14.4/62.7, Yakutsk=-52.8/-9.0/40.2, Yangon=-21.4/28.1/73.2, Yaoundé=-22.0/24.3/67.0, Yellowknife=-49.9/-4.3/40.9, Yerevan=-36.6/12.6/59.7, Yinchuan=-36.3/9.2/53.5, Zagreb=-34.2/10.9/56.0, Zanzibar City=-18.7/26.6/74.5, Zürich=-34.9/9.5/60.3, Ürümqi=-35.0/7.5/51.1, İzmir=-28.1/18.3/62.5}\n",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file, err := os.Open(tt.args.path)
			if err != nil {
				t.Error(err)
			}
			defer file.Close()

			var buf bytes.Buffer
			err = Process(file, &buf)
			if err != nil {
				t.Error(err)
			}

			if buf.String() != tt.args.expected {
				t.Errorf("expected: %s, got: %s", tt.args.expected, buf.String())
			}
		})
	}
}

func BenchmarkProcess(b *testing.B) {
	file, err := os.Open("/Users/ajitem/1brc/measurements-1b.txt")
	if err != nil {
		b.Error(err)
	}
	defer file.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err = Process(file, io.Discard)
		if err != nil {
			b.Error(err)
		}
	}
}
