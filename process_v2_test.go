package onebrc

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"
)

func TestV2_Process(t *testing.T) {
	type args struct {
		filePath string
	}
	tests := []struct {
		name    string
		args    args
		wantOut string
		wantErr bool
	}{
		{
			name: "TestV2_Process",
			args: args{
				filePath: "./testdata/measurements-10k.txt",
			},
			wantOut: "{Abha=3.10/21.03/35.50,Abidjan=12.20/26.40/41.80,Abéché=8.20/28.33/52.70,Accra=11.70/27.93/40.40,Addis Ababa=-3.90/18.67/38.20,Adelaide=1.30/16.58/35.90,Aden=14.00/28.98/50.00,Ahvaz=3.80/25.94/52.00,Albuquerque=-7.40/13.73/32.00,Alexandra=-16.40/7.02/21.50,Alexandria=2.70/21.21/35.50,Algiers=-4.20/19.31/40.10,Alice Springs=4.60/23.50/47.50,Almaty=-5.60/10.12/33.30,Amsterdam=-6.60/10.22/32.60,Anadyr=-33.10/-6.85/17.30,Anchorage=-20.20/-0.39/21.70,Andorra la Vella=-9.10/9.90/30.00,Ankara=-0.50/10.39/20.40,Antananarivo=-8.50/17.48/40.10,Antsiranana=8.50/25.36/39.60,Arkhangelsk=-26.30/4.36/21.20,Ashgabat=1.30/19.59/32.90,Asmara=0.40/19.37/37.30,Assab=2.80/31.09/56.00,Astana=-8.40/4.14/23.90,Athens=3.50/18.85/46.60,Atlanta=4.40/17.62/33.60,Auckland=-6.90/14.87/34.60,Austin=-2.10/21.14/35.80,Baghdad=2.50/22.07/39.80,Baguio=3.60/18.84/40.70,Baku=1.80/18.51/34.30,Baltimore=-13.40/8.20/26.20,Bamako=14.80/27.23/43.70,Bangkok=19.60/31.68/46.70,Bangui=1.20/20.84/33.10,Banjul=13.60/28.10/40.50,Barcelona=0.60/17.07/33.50,Bata=2.60/21.31/38.80,Batumi=2.40/13.59/25.30,Beijing=-9.80/11.42/31.70,Beirut=-3.50/17.73/32.90,Belgrade=-13.60/9.51/23.20,Belize City=5.40/23.46/52.50,Benghazi=-2.40/18.72/34.40,Bergen=-8.50/9.35/23.90,Berlin=-1.50/12.64/36.30,Bilbao=-2.70/13.69/29.90,Birao=5.80/27.26/47.20,Bishkek=-10.50/11.86/32.30,Bissau=9.00/25.69/45.60,Blantyre=4.60/25.23/49.30,Bloemfontein=-3.10/18.23/39.40,Boise=-12.00/10.01/34.10,Bordeaux=1.00/14.98/37.90,Bosaso=15.70/30.60/52.60,Boston=-13.70/12.51/29.10,Bouaké=7.30/25.96/62.80,Bratislava=-19.70/9.40/32.00,Brazzaville=13.10/27.65/40.20,Bridgetown=8.20/26.23/47.80,Brisbane=6.10/24.79/45.50,Brussels=-10.90/12.96/35.00,Bucharest=-3.60/14.16/32.30,Budapest=-11.30/6.06/17.60,Bujumbura=8.40/24.69/55.90,Bulawayo=6.90/19.81/33.70,Burnie=-12.70/13.85/26.20,Busan=-5.80/14.13/35.70,Cabo San Lucas=11.40/23.18/39.90,Cairns=10.60/28.24/48.50,Cairo=4.20/22.48/40.90,Calgary=-15.80/4.42/20.50,Canberra=-3.60/11.31/26.40,Cape Town=-6.80/15.84/31.60,Changsha=2.60/19.87/44.60,Charlotte=-3.50/17.27/34.80,Chiang Mai=5.00/27.88/53.30,Chicago=-10.00/11.45/26.50,Chihuahua=-0.60/18.17/37.30,Chittagong=-9.00/22.76/38.50,Chișinău=-8.00/9.22/23.30,Chongqing=-3.60/19.67/42.00,Christchurch=-5.80/13.77/42.60,City of San Marino=-6.00/8.85/31.00,Colombo=9.10/27.66/50.20,Columbus=-7.60/12.29/35.70,Conakry=6.50/24.09/44.30,Copenhagen=-18.80/5.93/25.80,Cotonou=6.80/27.87/47.10,Cracow=-4.60/10.87/32.40,Da Lat=1.60/17.68/37.20,Da Nang=-2.80/24.29/46.80,Dakar=4.40/20.37/44.60,Dallas=-3.40/13.97/32.60,Damascus=-3.30/17.66/39.10,Dampier=10.70/26.60/43.00,Dar es Salaam=11.00/27.73/51.70,Darwin=9.30/28.59/44.80,Denpasar=16.70/28.66/38.30,Denver=-4.10/12.37/35.60,Detroit=-4.20/13.28/36.00,Dhaka=4.70/21.26/34.70,Dikson=-26.60/-9.73/8.90,Dili=8.30/29.20/53.50,Djibouti=8.90/27.95/52.80,Dodoma=-3.40/18.20/40.60,Dolisie=1.50/20.68/41.50,Douala=5.70/27.00/42.90,Dubai=4.80/27.18/43.20,Dublin=-15.70/9.93/18.70,Dunedin=-3.40/11.72/29.30,Durban=6.20/20.88/45.70,Dushanbe=-3.80/13.38/34.00,Edinburgh=-13.50/8.42/20.00,Edmonton=-14.00/5.78/34.70,El Paso=3.10/17.37/32.20,Entebbe=3.10/22.49/42.40,Erbil=3.10/22.39/43.00,Erzurum=-16.40/4.51/22.70,Fairbanks=-18.70/0.74/23.80,Fianarantsoa=-4.30/15.22/30.40,Flores,  Petén=2.70/24.12/43.10,Frankfurt=-5.60/11.03/29.90,Fresno=-4.10/16.01/34.10,Fukuoka=-9.10/16.24/39.40,Gaborone=4.60/22.21/41.30,Gabès=-1.50/15.29/38.00,Gagnoa=4.40/26.23/46.10,Gangtok=-9.70/12.77/31.40,Garissa=13.80/27.24/45.00,Garoua=15.10/28.25/44.70,George Town=11.30/22.96/34.60,Ghanzi=4.30/21.81/40.80,Gjoa Haven=-29.80/-13.56/5.10,Guadalajara=3.30/22.76/38.10,Guangzhou=0.70/21.89/44.50,Guatemala City=1.00/18.28/30.50,Halifax=-13.20/2.60/18.50,Hamburg=-8.60/9.72/24.20,Hamilton=-3.70/12.26/24.60,Hanga Roa=-1.50/18.52/39.50,Hanoi=12.30/28.13/43.10,Harare=2.10/17.12/53.30,Harbin=-13.90/4.00/24.30,Hargeisa=1.80/25.78/45.70,Hat Yai=15.40/28.35/48.10,Havana=2.80/24.76/47.10,Helsinki=-6.00/9.19/25.20,Heraklion=10.80/24.13/42.40,Hiroshima=-4.20/15.19/33.10,Ho Chi Minh City=3.80/27.48/47.90,Hobart=-5.20/10.28/27.80,Hong Kong=3.90/24.06/41.50,Honiara=16.20/28.39/59.70,Honolulu=11.00/27.89/38.10,Houston=13.10/23.76/38.50,Ifrane=-9.50/11.94/28.20,Indianapolis=-6.50/13.12/32.90,Iqaluit=-36.60/-9.25/13.00,Irkutsk=-14.20/4.09/24.00,Istanbul=4.60/18.17/45.50,Jacksonville=2.00/21.98/39.50,Jakarta=9.20/26.95/43.50,Jayapura=2.10/26.47/51.10,Jerusalem=3.40/17.93/36.60,Johannesburg=4.60/16.43/33.50,Jos=6.60/25.92/49.10,Juba=12.50/28.66/51.90,Kabul=-8.70/12.87/36.80,Kampala=-1.60/18.31/43.90,Kandi=11.30/25.33/38.70,Kankan=10.00/26.28/57.00,Kano=6.20/23.31/45.30,Kansas City=-11.00/8.90/22.60,Karachi=5.20/25.59/42.80,Karonga=-7.40/23.88/44.30,Kathmandu=-5.40/19.41/39.70,Khartoum=15.20/30.87/55.30,Kingston=9.30/28.79/49.40,Kinshasa=3.00/23.37/45.60,Kolkata=5.40/27.52/47.10,Kuala Lumpur=11.50/28.48/43.70,Kumasi=16.20/27.02/43.00,Kunming=-8.70/17.15/33.00,Kuopio=-15.50/0.78/23.70,Kuwait City=2.30/23.74/42.60,Kyiv=-14.10/8.50/28.20,Kyoto=-2.50/13.65/31.70,La Ceiba=8.80/25.65/40.60,La Paz=7.60/25.43/42.20,Lagos=13.60/23.47/37.10,Lahore=3.20/23.04/39.20,Lake Havasu City=2.10/22.18/38.60,Lake Tekapo=-10.50/9.51/35.60,Las Palmas de Gran Canaria=0.00/20.95/37.90,Las Vegas=7.10/21.28/39.70,Launceston=-7.00/14.93/36.80,Lhasa=-16.50/5.16/13.70,Libreville=8.00/23.89/43.70,Lisbon=-3.90/15.71/39.80,Livingstone=11.50/24.47/40.10,Ljubljana=-6.80/8.86/23.60,Lodwar=7.80/28.00/40.90,Lomé=7.70/26.56/49.20,London=-10.50/9.34/24.30,Los Angeles=3.30/20.64/39.90,Louisville=-5.00/11.49/30.30,Luanda=9.10/24.51/42.80,Lubumbashi=4.20/21.97/44.40,Lusaka=-7.20/18.31/34.30,Luxembourg City=-8.10/8.93/34.20,Lviv=-14.90/3.64/19.30,Lyon=-10.00/11.01/34.50,Madrid=-2.90/18.21/40.30,Mahajanga=7.80/24.28/47.10,Makassar=-1.70/22.91/43.40,Makurdi=5.10/22.49/40.50,Malabo=9.20/24.64/44.90,Malé=9.30/29.11/42.10,Managua=1.60/26.16/42.30,Manama=1.80/27.68/50.80,Mandalay=13.40/26.17/37.10,Mango=8.20/24.90/44.40,Manila=12.70/29.24/45.50,Maputo=4.70/25.89/55.90,Marrakesh=0.80/19.62/51.00,Marseille=-9.50/15.66/36.90,Maun=-5.70/22.20/38.30,Medan=8.50/28.13/45.80,Mek'ele=7.50/23.55/39.20,Melbourne=-3.10/16.67/36.50,Memphis=-3.40/19.89/50.40,Mexicali=2.90/18.29/32.20,Mexico City=-6.20/19.65/48.80,Miami=4.50/25.31/51.40,Milan=-14.40/12.82/32.90,Milwaukee=-2.10/10.03/32.70,Minneapolis=-10.10/7.08/26.50,Minsk=-12.90/7.63/40.60,Mogadishu=5.00/26.23/44.00,Mombasa=-6.50/21.46/46.80,Monaco=0.80/18.40/32.40,Moncton=-10.40/3.44/19.70,Monterrey=2.50/20.87/39.00,Montreal=-10.70/5.78/28.10,Moscow=-9.00/6.93/30.00,Mumbai=3.80/30.51/51.40,Murmansk=-14.30/1.99/13.70,Muscat=6.80/29.83/55.10,Mzuzu=-0.80/17.22/43.30,N'Djamena=3.80/28.32/49.20,Naha=-3.00/20.65/41.30,Nairobi=-8.90/14.71/38.00,Nakhon Ratchasima=12.60/28.16/41.80,Napier=-1.60/15.88/31.50,Napoli=-7.20/20.85/46.10,Nashville=3.30/17.73/30.80,Nassau=6.30/25.17/42.40,Ndola=1.90/15.66/40.30,New Delhi=3.50/24.49/50.40,New Orleans=3.70/21.80/42.50,New York City=-3.80/13.83/32.20,Ngaoundéré=1.30/17.47/34.40,Niamey=8.50/29.22/47.00,Nicosia=0.20/19.44/34.70,Niigata=-2.30/15.91/47.10,Nouadhibou=3.80/22.43/50.30,Nouakchott=-2.30/24.55/47.30,Novosibirsk=-15.10/-0.45/26.90,Nuuk=-21.20/-0.72/20.00,Odesa=-10.50/9.30/34.90,Odienné=2.10/22.03/34.30,Oklahoma City=-10.60/15.44/30.70,Omaha=-6.70/11.34/31.00,Oranjestad=6.10/31.51/58.90,Oslo=-7.30/11.09/24.40,Ottawa=-7.10/9.09/28.40,Ouagadougou=4.80/29.69/45.80,Ouahigouya=6.30/26.45/42.10,Ouarzazate=-7.60/16.73/35.40,Oulu=-20.70/1.89/20.30,Palembang=13.50/26.95/47.70,Palermo=-1.20/19.39/34.50,Palm Springs=3.60/20.58/39.20,Palmerston North=-8.80/13.12/30.10,Panama City=14.20/29.55/46.60,Parakou=2.90/25.73/39.20,Paris=-5.60/11.54/31.60,Perth=2.40/14.98/39.00,Petropavlovsk-Kamchatsky=-28.00/2.32/21.90,Philadelphia=-4.50/14.68/29.10,Phnom Penh=17.80/30.06/53.30,Phoenix=-2.60/22.50/41.70,Pittsburgh=-5.80/11.25/26.50,Podgorica=-5.00/13.78/26.90,Pointe-Noire=10.20/23.57/35.90,Pontianak=11.30/28.97/58.80,Port Moresby=8.60/25.42/54.10,Port Sudan=8.90/28.42/49.10,Port Vila=7.10/26.12/44.60,Port-Gentil=0.80/26.41/41.30,Portland (OR)=-8.20/10.13/30.10,Porto=-8.90/14.39/42.00,Prague=-14.20/10.94/28.50,Praia=5.00/25.11/38.70,Pretoria=0.80/18.46/35.00,Pyongyang=-13.20/10.02/31.90,Rabat=1.40/15.33/37.10,Rangpur=12.20/25.51/44.90,Reggane=11.10/30.25/57.20,Reykjavík=-19.00/3.26/27.60,Riga=-18.10/2.47/19.20,Riyadh=11.70/28.38/46.40,Rome=-2.30/18.34/38.00,Roseau=11.60/25.40/45.00,Rostov-on-Don=-3.60/9.50/25.80,Sacramento=-1.60/16.31/33.80,Saint Petersburg=-1.10/8.95/25.00,Saint-Pierre=-18.00/6.40/27.60,Salt Lake City=-8.70/10.40/25.20,San Antonio=-0.50/19.21/33.10,San Diego=-4.50/20.47/34.20,San Francisco=-8.60/14.80/51.50,San Jose=-9.30/16.19/30.50,San José=0.80/23.94/43.50,San Juan=10.40/26.48/49.70,San Salvador=5.30/21.77/41.90,Sana'a=-0.50/20.71/33.90,Santo Domingo=7.60/27.87/43.70,Sapporo=-3.00/12.10/26.40,Sarajevo=-5.40/10.47/22.90,Saskatoon=-17.20/-0.65/15.20,Seattle=-18.40/8.65/34.10,Seoul=-22.40/10.00/29.10,Seville=6.60/19.13/32.20,Shanghai=-2.80/14.92/31.90,Singapore=14.00/29.22/47.80,Skopje=-3.10/9.66/32.70,Sochi=-3.00/13.37/32.30,Sofia=-10.60/9.37/29.70,Sokoto=-2.70/28.24/46.00,Split=-13.70/15.84/34.20,St. John's=-23.30/4.63/26.50,St. Louis=-1.50/14.94/32.90,Stockholm=-20.10/5.61/32.90,Surabaya=7.80/27.87/48.10,Suva=8.80/26.77/40.30,Suwałki=-12.90/4.59/27.00,Sydney=2.30/17.23/40.70,Ségou=8.60/29.24/68.20,Tabora=11.40/25.23/46.70,Tabriz=-4.00/11.59/30.00,Taipei=-2.70/21.60/42.50,Tallinn=-17.80/5.23/17.50,Tamale=13.50/27.48/49.80,Tamanrasset=4.90/21.71/40.40,Tampa=9.90/23.70/35.10,Tashkent=0.30/14.75/36.40,Tauranga=-7.70/16.56/36.70,Tbilisi=-5.80/14.16/33.90,Tegucigalpa=7.80/22.48/40.20,Tehran=-4.60/20.20/37.40,Tel Aviv=0.40/17.82/30.30,Thessaloniki=2.90/16.29/40.80,Thiès=6.30/24.74/45.60,Tijuana=-5.60/19.66/46.00,Timbuktu=9.70/29.95/47.90,Tirana=-6.00/11.82/29.00,Toamasina=2.40/21.52/46.70,Tokyo=0.40/17.39/34.50,Toliara=6.50/24.89/40.60,Toluca=-4.50/11.82/30.20,Toronto=-12.90/9.18/35.50,Tripoli=-5.40/21.80/49.10,Tromsø=-9.40/-1.31/10.50,Tucson=0.30/21.36/42.30,Tunis=7.40/19.39/36.20,Ulaanbaatar=-22.40/-2.03/16.00,Upington=0.90/19.02/42.30,Vaduz=-12.00/12.04/28.60,Valencia=-8.20/13.28/35.80,Valletta=-13.80/18.29/42.60,Vancouver=-21.20/9.17/33.80,Veracruz=3.80/24.54/40.70,Vienna=-3.00/6.09/21.20,Vientiane=11.70/27.48/44.50,Villahermosa=13.30/27.85/53.40,Vilnius=-17.10/3.65/24.00,Virginia Beach=-6.40/15.24/35.20,Vladivostok=-15.30/7.55/25.90,Warsaw=-11.60/6.92/32.80,Washington, D.C.=-8.90/12.83/25.30,Wau=9.50/30.54/57.70,Wellington=-7.80/12.35/23.80,Whitehorse=-18.90/-6.15/11.10,Wichita=-6.20/12.67/35.50,Willemstad=12.30/25.59/42.30,Winnipeg=-12.60/5.87/20.70,Wrocław=-12.60/12.92/32.50,Xi'an=-4.60/14.00/27.10,Yakutsk=-24.30/-9.95/16.60,Yangon=9.40/29.41/51.10,Yaoundé=-1.50/22.93/43.80,Yellowknife=-28.20/-6.37/6.10,Yerevan=-9.50/12.22/32.00,Yinchuan=-12.90/7.85/29.00,Zagreb=-14.70/11.07/25.90,Zanzibar City=7.50/24.43/48.30,Zürich=-15.80/8.03/37.50,Ürümqi=-10.40/8.07/28.00,İzmir=7.40/20.05/39.40}\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file, err := os.Open(tt.args.filePath)
			if err != nil {
				t.Error(err)
			}
			p := V2{}
			out := &bytes.Buffer{}
			err = p.Process(file, out)
			if (err != nil) != tt.wantErr {
				t.Errorf("Process() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotOut := out.String(); gotOut != tt.wantOut {
				t.Errorf("Process() gotOut = %v, want %v", gotOut, tt.wantOut)
			}
		})
	}
}

func BenchmarkProcessV2(b *testing.B) {
	file, err := os.Open("./testdata/measurements.txt")
	if err != nil {
		b.Error(err)
	}

	defer file.Close()

	var out bytes.Buffer

	for i := 0; i < b.N; i++ {
		p := V2{}
		out := &bytes.Buffer{}
		err = p.Process(file, out)
		if err != nil {
			b.Error(err)
		}
	}

	_, _ = fmt.Fprintf(io.Discard, "%s", out.String())
}
