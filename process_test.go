package onebrc

import (
	"bytes"
	"io"
	"os"
	"reflect"
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
			name: "1m",
			args: args{
				path:     "/Users/ajitem/1brc/measurements-1m.txt",
				expected: "{Abha=-29.0/18.0/62.7, Abidjan=-16.7/26.0/74.1, Abéché=-15.7/29.4/76.1, Accra=-22.4/26.4/73.3, Addis Ababa=-30.5/16.0/59.7, Adelaide=-30.7/17.3/60.5, Aden=-18.0/29.1/77.8, Ahvaz=-22.7/25.4/71.5, Albuquerque=-31.5/14.0/62.1, Alexandra=-32.8/11.0/64.0, Alexandria=-23.8/20.0/66.2, Algiers=-24.6/18.2/61.2, Alice Springs=-27.2/21.0/64.4, Almaty=-34.4/10.0/56.4, Amsterdam=-36.9/10.2/55.7, Anadyr=-53.3/-6.9/39.3, Anchorage=-40.4/2.8/47.6, Andorra la Vella=-41.1/9.8/58.1, Ankara=-36.6/12.0/59.4, Antananarivo=-29.1/17.9/62.4, Antsiranana=-22.5/25.2/71.4, Arkhangelsk=-42.4/1.3/45.2, Ashgabat=-29.9/17.1/65.2, Asmara=-29.3/15.6/60.5, Assab=-15.6/30.5/73.4, Astana=-44.4/3.5/49.6, Athens=-25.0/19.2/64.7, Atlanta=-27.1/17.0/62.9, Auckland=-32.1/15.2/63.3, Austin=-24.7/20.7/65.4, Baghdad=-20.3/22.8/70.4, Baguio=-24.7/19.5/62.7, Baku=-28.5/15.1/62.8, Baltimore=-31.6/13.1/58.9, Bamako=-20.6/27.8/72.5, Bangkok=-15.2/28.6/75.2, Bangui=-19.0/26.0/70.4, Banjul=-17.7/26.0/70.0, Barcelona=-34.5/18.2/64.8, Bata=-20.1/25.1/70.2, Batumi=-32.7/14.0/56.8, Beijing=-32.4/12.9/56.8, Beirut=-24.8/20.9/71.5, Belgrade=-38.5/12.5/54.4, Belize City=-16.2/26.7/71.2, Benghazi=-24.5/19.9/65.3, Bergen=-45.8/7.7/53.6, Berlin=-32.3/10.3/55.9, Bilbao=-29.7/14.7/58.2, Birao=-15.3/26.5/71.5, Bishkek=-32.3/11.3/56.1, Bissau=-17.0/27.0/70.9, Blantyre=-22.2/22.2/65.6, Bloemfontein=-30.5/15.6/61.7, Boise=-34.6/11.4/56.0, Bordeaux=-32.7/14.2/63.4, Bosaso=-15.8/30.0/76.0, Boston=-32.8/10.9/56.5, Bouaké=-25.7/26.0/70.5, Bratislava=-30.8/10.5/57.9, Brazzaville=-18.5/25.0/77.5, Bridgetown=-22.2/27.0/73.2, Brisbane=-21.1/21.4/67.8, Brussels=-37.6/10.5/56.7, Bucharest=-31.8/10.8/54.5, Budapest=-33.8/11.3/62.3, Bujumbura=-26.3/23.8/70.2, Bulawayo=-25.2/18.9/70.2, Burnie=-34.2/13.1/55.4, Busan=-29.5/15.0/61.5, Cabo San Lucas=-20.9/23.9/70.7, Cairns=-20.1/25.0/74.6, Cairo=-25.2/21.4/71.1, Calgary=-40.8/4.4/56.6, Canberra=-35.2/13.1/61.6, Cape Town=-28.9/16.2/66.9, Changsha=-29.6/17.4/60.9, Charlotte=-29.6/16.1/63.8, Chiang Mai=-18.3/25.8/74.7, Chicago=-34.1/9.8/55.0, Chihuahua=-26.6/18.6/67.7, Chittagong=-20.1/25.9/72.6, Chișinău=-31.8/10.2/57.1, Chongqing=-28.1/18.6/61.0, Christchurch=-31.1/12.2/56.1, City of San Marino=-31.1/11.8/55.9, Colombo=-20.9/27.4/73.6, Columbus=-37.7/11.7/60.2, Conakry=-16.0/26.4/71.9, Copenhagen=-37.9/9.1/53.4, Cotonou=-18.3/27.2/75.1, Cracow=-37.7/9.3/50.1, Da Lat=-31.1/17.9/60.1, Da Nang=-23.0/25.8/69.0, Dakar=-22.3/24.0/73.3, Dallas=-24.3/19.0/67.7, Damascus=-28.1/17.0/68.6, Dampier=-22.3/26.4/68.7, Dar es Salaam=-20.2/25.8/72.1, Darwin=-16.1/27.6/79.5, Denpasar=-19.5/23.7/70.2, Denver=-42.7/10.4/55.9, Detroit=-32.2/10.0/52.4, Dhaka=-26.5/25.9/70.9, Dikson=-61.8/-11.1/33.2, Dili=-19.4/26.6/71.2, Djibouti=-16.1/29.9/73.9, Dodoma=-20.7/22.7/66.3, Dolisie=-25.4/24.0/72.0, Douala=-19.3/26.7/76.5, Dubai=-25.4/26.9/70.9, Dublin=-38.8/9.8/58.7, Dunedin=-32.8/11.1/54.1, Durban=-22.4/20.6/67.3, Dushanbe=-31.8/14.7/58.9, Edinburgh=-34.5/9.3/56.6, Edmonton=-41.5/4.2/45.9, El Paso=-25.5/18.1/64.8, Entebbe=-21.7/21.0/63.4, Erbil=-24.4/19.5/68.7, Erzurum=-43.1/5.1/48.5, Fairbanks=-52.3/-2.3/50.9, Fianarantsoa=-27.6/17.9/62.9, Flores,  Petén=-17.8/26.4/70.6, Frankfurt=-32.0/10.6/56.1, Fresno=-37.4/17.9/62.8, Fukuoka=-27.5/17.0/67.6, Gaborone=-22.1/21.0/65.2, Gabès=-24.5/19.5/64.0, Gagnoa=-21.2/26.0/70.6, Gangtok=-29.4/15.2/60.2, Garissa=-20.1/29.3/74.2, Garoua=-18.2/28.3/70.9, George Town=-15.8/27.9/69.2, Ghanzi=-22.0/21.4/64.9, Gjoa Haven=-56.3/-14.4/26.1, Guadalajara=-25.1/20.9/64.7, Guangzhou=-28.3/22.4/66.1, Guatemala City=-24.2/20.4/62.8, Halifax=-34.9/7.5/53.7, Hamburg=-36.1/9.7/55.6, Hamilton=-29.8/13.8/59.3, Hanga Roa=-25.7/20.5/66.3, Hanoi=-21.1/23.6/74.7, Harare=-30.3/18.4/62.9, Harbin=-41.3/5.0/49.9, Hargeisa=-26.1/21.7/64.8, Hat Yai=-16.6/27.0/76.4, Havana=-18.4/25.2/74.9, Helsinki=-37.4/5.9/50.3, Heraklion=-28.5/18.9/64.0, Hiroshima=-27.4/16.3/60.8, Ho Chi Minh City=-18.1/27.4/74.1, Hobart=-33.1/12.7/58.4, Hong Kong=-23.2/23.3/70.2, Honiara=-20.6/26.5/77.0, Honolulu=-18.5/25.4/73.2, Houston=-25.9/20.8/64.9, Ifrane=-30.2/11.4/64.3, Indianapolis=-35.7/11.8/63.1, Iqaluit=-52.4/-9.3/36.1, Irkutsk=-43.4/1.0/44.1, Istanbul=-29.6/13.9/55.7, Jacksonville=-28.9/20.3/64.7, Jakarta=-19.1/26.7/70.4, Jayapura=-20.1/27.0/72.9, Jerusalem=-25.5/18.3/61.8, Johannesburg=-26.0/15.5/62.0, Jos=-22.8/22.8/66.9, Juba=-15.5/27.8/73.5, Kabul=-29.9/12.1/52.3, Kampala=-30.8/19.9/68.8, Kandi=-15.3/27.7/75.8, Kankan=-19.7/26.5/70.1, Kano=-25.2/26.4/69.4, Kansas City=-31.1/12.5/56.2, Karachi=-16.4/26.0/84.3, Karonga=-18.2/24.4/67.7, Kathmandu=-24.9/18.3/59.9, Khartoum=-19.0/29.9/74.7, Kingston=-29.1/27.4/78.3, Kinshasa=-18.0/25.3/72.4, Kolkata=-18.6/26.7/69.8, Kuala Lumpur=-15.8/27.3/69.9, Kumasi=-17.6/26.0/71.9, Kunming=-32.2/15.7/66.0, Kuopio=-47.2/3.4/52.8, Kuwait City=-21.0/25.7/72.8, Kyiv=-42.4/8.4/54.8, Kyoto=-33.8/15.8/59.6, La Ceiba=-21.0/26.2/69.4, La Paz=-22.6/23.7/71.1, Lagos=-18.4/26.8/70.7, Lahore=-18.2/24.3/68.1, Lake Havasu City=-23.3/23.7/75.0, Lake Tekapo=-32.5/8.7/53.1, Las Palmas de Gran Canaria=-20.8/21.2/66.5, Las Vegas=-28.2/20.3/62.6, Launceston=-31.7/13.1/58.1, Lhasa=-35.6/7.7/52.0, Libreville=-24.3/25.9/72.1, Lisbon=-26.7/17.5/59.4, Livingstone=-23.7/21.8/68.2, Ljubljana=-32.4/10.9/56.3, Lodwar=-17.7/29.3/75.8, Lomé=-18.1/26.9/76.7, London=-37.3/11.3/55.7, Los Angeles=-25.6/18.6/65.1, Louisville=-31.5/13.9/62.0, Luanda=-18.2/25.8/69.6, Lubumbashi=-21.7/20.8/65.2, Lusaka=-23.4/19.9/69.8, Luxembourg City=-35.3/9.3/58.2, Lviv=-38.9/7.8/49.9, Lyon=-32.3/12.5/57.5, Madrid=-25.2/15.0/59.7, Mahajanga=-21.1/26.3/71.1, Makassar=-23.8/26.7/70.1, Makurdi=-17.7/26.0/71.1, Malabo=-24.7/26.3/70.4, Malé=-14.7/28.0/74.2, Managua=-17.4/27.3/72.2, Manama=-20.0/26.5/72.3, Mandalay=-16.6/28.0/72.1, Mango=-20.2/28.1/72.6, Manila=-14.4/28.4/70.7, Maputo=-18.8/22.8/63.0, Marrakesh=-26.9/19.6/66.0, Marseille=-33.4/15.8/65.1, Maun=-19.8/22.4/64.0, Medan=-19.8/26.5/70.6, Mek'ele=-22.1/22.7/68.4, Melbourne=-32.3/15.1/60.1, Memphis=-27.5/17.2/60.3, Milan=-30.6/13.0/57.5, Minneapolis=-34.9/7.8/52.9, Minsk=-42.8/6.7/48.7, Mombasa=-17.5/26.4/70.4, Monaco=-26.6/16.4/64.8, Mexicali=-24.7/23.1/67.2, Mexico City=-25.0/17.5/62.9, Miami=-22.6/24.9/66.5, Milwaukee=-33.3/8.9/53.1, Mogadishu=-16.5/27.1/74.2, Moncton=-39.0/6.1/52.4, Monterrey=-23.9/22.3/65.6, Montreal=-35.7/6.8/50.9, Moscow=-42.0/5.8/51.7, Mumbai=-19.2/27.1/68.0, Murmansk=-41.3/0.6/44.4, Muscat=-15.4/28.0/75.1, Mzuzu=-32.2/17.7/61.5, N'Djamena=-14.4/28.3/76.1, Naha=-24.9/23.1/69.2, Nairobi=-27.7/17.8/65.3, Nakhon Ratchasima=-14.3/27.3/71.8, Napier=-28.7/14.6/58.5, Napoli=-28.6/15.9/63.8, Nashville=-28.4/15.4/62.5, Nassau=-20.4/24.6/67.6, Ndola=-24.9/20.3/68.9, New Delhi=-21.4/25.0/69.4, New Orleans=-26.3/20.7/64.2, New York City=-31.1/12.9/57.0, Ngaoundéré=-24.4/22.0/65.1, Niamey=-16.1/29.3/73.3, Nicosia=-26.6/19.7/63.8, Niigata=-33.7/13.9/58.0, Nouadhibou=-25.6/21.3/65.8, Nouakchott=-21.5/25.7/70.8, Novosibirsk=-50.1/1.7/51.5, Nuuk=-43.9/-1.4/44.8, Odesa=-40.2/10.7/54.3, Odienné=-20.4/26.0/70.6, Oklahoma City=-25.6/15.9/61.3, Omaha=-37.7/10.6/57.8, Oranjestad=-16.6/28.1/73.3, Oslo=-35.6/5.7/53.2, Ottawa=-38.0/6.6/53.7, Ouagadougou=-18.2/28.3/73.4, Ouahigouya=-19.8/28.6/73.8, Ouarzazate=-27.6/18.9/62.2, Oulu=-42.3/2.7/49.4, Palembang=-19.3/27.3/75.2, Palermo=-27.0/18.5/60.3, Palm Springs=-21.8/24.5/67.7, Palmerston North=-32.3/13.2/60.4, Panama City=-14.3/28.0/71.4, Parakou=-16.8/26.8/70.7, Paris=-36.6/12.3/56.3, Perth=-27.9/18.7/60.3, Petropavlovsk-Kamchatsky=-42.9/1.9/46.1, Philadelphia=-29.8/13.2/59.4, Phnom Penh=-16.2/28.3/81.6, Phoenix=-29.2/23.9/68.8, Pittsburgh=-34.9/10.8/52.2, Podgorica=-32.2/15.3/60.2, Pointe-Noire=-19.5/26.1/72.8, Pontianak=-15.3/27.7/74.6, Port Moresby=-16.5/26.9/71.2, Port Sudan=-15.1/28.4/76.6, Port Vila=-23.6/24.3/66.2, Port-Gentil=-25.8/26.0/70.7, Portland (OR)=-31.2/12.4/57.7, Porto=-30.9/15.7/61.3, Prague=-35.8/8.4/52.0, Praia=-19.5/24.4/69.6, Pretoria=-28.0/18.2/65.2, Pyongyang=-36.7/10.8/55.0, Rabat=-30.2/17.2/60.6, Rangpur=-17.9/24.4/70.6, Reggane=-19.6/28.3/73.4, Reykjavík=-38.0/4.3/49.3, Riga=-39.0/6.2/49.4, Riyadh=-17.0/26.0/76.1, Rome=-30.3/15.2/60.2, Roseau=-17.4/26.2/71.6, Rostov-on-Don=-35.4/9.9/54.1, Sacramento=-32.0/16.3/61.0, Saint Petersburg=-36.9/5.8/47.7, Saint-Pierre=-41.5/5.7/50.5, Salt Lake City=-34.6/11.6/58.1, San Antonio=-21.9/20.8/68.4, San Diego=-25.5/17.8/70.8, San Francisco=-27.7/14.6/68.7, San Jose=-27.2/16.4/60.8, San José=-22.6/22.6/69.3, San Juan=-16.1/27.2/75.2, San Salvador=-24.8/23.1/70.4, Sana'a=-25.1/20.0/66.2, Santo Domingo=-20.4/25.9/69.6, Sapporo=-36.0/8.9/53.6, Sarajevo=-35.8/10.1/53.3, Saskatoon=-51.9/3.3/52.7, Seattle=-35.1/11.3/57.0, Seoul=-40.3/12.5/56.1, Seville=-25.4/19.2/63.8, Shanghai=-27.7/16.7/60.2, Singapore=-19.0/27.0/73.9, Skopje=-31.2/12.4/56.5, Sochi=-32.4/14.2/62.3, Sofia=-36.5/10.6/54.1, Sokoto=-23.6/28.0/72.9, Split=-26.7/16.1/62.2, St. John's=-46.7/5.0/52.3, St. Louis=-39.5/13.9/57.0, Stockholm=-39.0/6.6/50.1, Surabaya=-16.8/27.1/70.5, Suva=-18.8/25.6/71.9, Suwałki=-38.5/7.2/57.5, Sydney=-23.0/17.6/59.8, Ségou=-14.2/28.0/71.2, Tabora=-23.5/23.0/69.7, Tabriz=-33.5/12.6/62.1, Taipei=-23.7/23.0/69.8, Tallinn=-37.1/6.4/51.4, Tamale=-16.4/27.9/74.0, Tamanrasset=-26.8/21.7/62.3, Tampa=-22.6/22.9/64.0, Tashkent=-29.7/14.8/59.3, Tauranga=-29.6/14.8/59.9, Tbilisi=-38.4/12.9/60.4, Tegucigalpa=-22.0/21.7/67.0, Tehran=-32.3/17.0/64.3, Tel Aviv=-22.4/20.0/64.8, Thessaloniki=-28.8/16.0/63.3, Thiès=-23.1/24.0/68.4, Tijuana=-25.7/17.8/63.0, Timbuktu=-15.3/28.0/71.3, Tirana=-30.5/15.2/63.0, Toamasina=-23.1/23.4/72.2, Tokyo=-29.0/15.4/63.1, Toliara=-18.0/24.0/68.1, Toluca=-31.0/12.4/62.9, Toronto=-40.1/9.4/54.2, Tripoli=-23.2/20.0/64.5, Tromsø=-42.0/2.9/53.1, Tucson=-22.3/21.0/64.0, Tunis=-29.1/18.4/68.8, Ulaanbaatar=-46.4/-0.4/42.7, Upington=-19.6/20.4/64.6, Vaduz=-33.3/10.1/54.1, Valencia=-28.7/18.3/64.1, Valletta=-26.4/18.8/64.0, Vancouver=-34.3/10.4/57.2, Veracruz=-18.5/25.4/74.5, Vienna=-34.4/10.4/55.4, Vientiane=-19.0/25.9/71.2, Villahermosa=-17.1/27.1/71.5, Vilnius=-37.1/6.0/53.3, Virginia Beach=-29.2/15.8/61.4, Vladivostok=-40.9/4.9/47.1, Warsaw=-41.9/8.5/53.9, Washington, D.C.=-30.5/14.6/62.6, Wau=-15.7/27.8/75.9, Wellington=-35.1/12.9/59.7, Whitehorse=-43.8/-0.1/49.4, Wichita=-30.3/13.9/58.2, Willemstad=-20.0/28.0/70.9, Winnipeg=-39.1/3.0/55.8, Wrocław=-36.1/9.6/56.7, Yellowknife=-47.0/-4.3/38.6, Yinchuan=-36.3/9.0/53.5, Zanzibar City=-18.7/26.0/74.5, Ürümqi=-35.0/7.4/51.1, Xi'an=-30.0/14.1/62.7, Yakutsk=-52.8/-8.8/40.2, Yangon=-21.4/27.5/73.2, Yaoundé=-22.0/23.8/67.0, Yellowknife=-49.9/-4.3/40.9, Yerevan=-36.6/12.4/59.7, Zagreb=-34.2/10.7/56.0, Zürich=-34.9/9.3/60.3, İzmir=-28.1/17.9/62.5}\n",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := Process(tt.args.path, &buf)
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
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := Process("/Users/ajitem/1brc/measurements-1b.txt", io.Discard)
		if err != nil {
			b.Error(err)
		}
	}
}

func Test_splitFile(t *testing.T) {
	type args struct {
		path     string
		numParts int
	}
	tests := []struct {
		name    string
		args    args
		want    []metadata
		wantErr bool
	}{
		{
			name: "10k",
			args: args{
				path:     "/Users/ajitem/1brc/measurements.txt",
				numParts: 10,
			},
			want: []metadata{
				{0, 13788}, {13788, 13793}, {27581, 13784}, {41365, 13792},
				{55157, 13796}, {68953, 13796}, {82749, 13791}, {96540, 13794},
				{110334, 13794}, {124128, 13847},
			},
		},
		{
			name: "100m",
			args: args{
				path:     "/Users/ajitem/1brc/measurements-1m.txt",
				numParts: 10,
			},
			want: []metadata{
				{0, 137954157}, {137954157, 137954149}, {275908306, 137954153},
				{413862459, 137954142}, {551816601, 137954149}, {689770750, 137954142},
				{827724892, 137954146}, {965679038, 137954155}, {1103633193, 137954146},
				{1241587339, 137954246},
			},
		},
		{
			name: "1b",
			args: args{
				path:     "/Users/ajitem/1brc/measurements-1b.txt",
				numParts: 10,
			},
			want: []metadata{
				{0, 1379545341}, {1379545341, 1379545344}, {2759090685, 1379545349},
				{4138636034, 1379545345}, {5518181379, 1379545345}, {6897726724, 1379545344},
				{8277272068, 1379545345}, {9656817413, 1379545355}, {11036362768, 1379545348},
				{12415908116, 1379545446},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := splitChunks(tt.args.path, tt.args.numParts)
			if (err != nil) != tt.wantErr {
				t.Errorf("splitChunks() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("splitChunks() got = %v, want %v", got, tt.want)
			}
			totalSplitSize := int64(0)
			f, err := os.Open(tt.args.path)
			if err != nil {
				t.Error(err)
			}
			defer f.Close()

			for _, p := range got {
				_, err = f.Seek(p.offset, io.SeekStart)
				if err != nil {
					t.Error(err)
				}

				buf := make([]byte, p.size)

				_, err = io.ReadFull(f, buf)
				if err != nil {
					t.Error(err)
				}

				// the first character should not be a number and the last character should be a newline
				if (buf[0] >= '0' && buf[0] <= '9') && buf[len(buf)-1] != '\n' {
					t.Errorf("expected first character to be a letter and last character to be a newline")
				}

				totalSplitSize += p.size
			}

			st, err := f.Stat()
			if err != nil {
				t.Error(err)
			}

			if totalSplitSize != st.Size() {
				t.Errorf("expected total split size to be equal to file size")
			}
		})
	}
}
