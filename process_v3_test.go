package onebrc

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"
)

func TestV3_Process(t *testing.T) {
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
			name: "TestV3_Process",
			args: args{
				filePath: "./testdata/measurements-10k.txt",
			},
			wantOut: "{Abha=3.1/21.0/35.5,Abidjan=12.2/26.4/41.8,Abéché=8.2/28.3/52.7,Accra=11.7/27.9/40.4,Addis Ababa=-3.9/18.7/38.2,Adelaide=1.3/16.6/35.9,Aden=14.0/29.0/50.0,Ahvaz=3.8/25.9/52.0,Albuquerque=-7.4/13.7/32.0,Alexandra=-16.4/7.0/21.5,Alexandria=2.7/21.2/35.5,Algiers=-4.2/19.3/40.1,Alice Springs=4.6/23.5/47.5,Almaty=-5.6/10.1/33.3,Amsterdam=-6.6/10.2/32.6,Anadyr=-33.1/-6.9/17.3,Anchorage=-20.2/-0.4/21.7,Andorra la Vella=-9.1/9.9/30.0,Ankara=-0.5/10.4/20.4,Antananarivo=-8.5/17.5/40.1,Antsiranana=8.5/25.4/39.6,Arkhangelsk=-26.3/4.4/21.2,Ashgabat=1.3/19.6/32.9,Asmara=0.4/19.4/37.3,Assab=2.8/31.1/56.0,Astana=-8.4/4.1/23.9,Athens=3.5/18.9/46.6,Atlanta=4.4/17.6/33.6,Auckland=-6.9/14.9/34.6,Austin=-2.1/21.1/35.8,Baghdad=2.5/22.1/39.8,Baguio=3.6/18.8/40.7,Baku=1.8/18.5/34.3,Baltimore=-13.4/8.2/26.2,Bamako=14.8/27.2/43.7,Bangkok=19.6/31.7/46.7,Bangui=1.2/20.8/33.1,Banjul=13.6/28.1/40.5,Barcelona=0.6/17.1/33.5,Bata=2.6/21.3/38.8,Batumi=2.4/13.6/25.3,Beijing=-9.8/11.4/31.7,Beirut=-3.5/17.7/32.9,Belgrade=-13.6/9.5/23.2,Belize City=5.4/23.5/52.5,Benghazi=-2.4/18.7/34.4,Bergen=-8.5/9.3/23.9,Berlin=-1.5/12.6/36.3,Bilbao=-2.7/13.7/29.9,Birao=5.8/27.3/47.2,Bishkek=-10.5/11.9/32.3,Bissau=9.0/25.7/45.6,Blantyre=4.6/25.2/49.3,Bloemfontein=-3.1/18.2/39.4,Boise=-12.0/10.0/34.1,Bordeaux=1.0/15.0/37.9,Bosaso=15.7/30.6/52.6,Boston=-13.7/12.5/29.1,Bouaké=7.3/26.0/62.8,Bratislava=-19.7/9.4/32.0,Brazzaville=13.1/27.7/40.2,Bridgetown=8.2/26.2/47.8,Brisbane=6.1/24.8/45.5,Brussels=-10.9/13.0/35.0,Bucharest=-3.6/14.2/32.3,Budapest=-11.3/6.1/17.6,Bujumbura=8.4/24.7/55.9,Bulawayo=6.9/19.8/33.7,Burnie=-12.7/13.9/26.2,Busan=-5.8/14.1/35.7,Cabo San Lucas=11.4/23.2/39.9,Cairns=10.6/28.2/48.5,Cairo=4.2/22.5/40.9,Calgary=-15.8/4.4/20.5,Canberra=-3.6/11.3/26.4,Cape Town=-6.8/15.8/31.6,Changsha=2.6/19.9/44.6,Charlotte=-3.5/17.3/34.8,Chiang Mai=5.0/27.9/53.3,Chicago=-10.0/11.4/26.5,Chihuahua=-0.6/18.2/37.3,Chittagong=-9.0/22.8/38.5,Chișinău=-8.0/9.2/23.3,Chongqing=-3.6/19.7/42.0,Christchurch=-5.8/13.8/42.6,City of San Marino=-6.0/8.8/31.0,Colombo=9.1/27.7/50.2,Columbus=-7.6/12.3/35.7,Conakry=6.5/24.1/44.3,Copenhagen=-18.8/5.9/25.8,Cotonou=6.8/27.9/47.1,Cracow=-4.6/10.9/32.4,Da Lat=1.6/17.7/37.2,Da Nang=-2.8/24.3/46.8,Dakar=4.4/20.4/44.6,Dallas=-3.4/14.0/32.6,Damascus=-3.3/17.7/39.1,Dampier=10.7/26.6/43.0,Dar es Salaam=11.0/27.7/51.7,Darwin=9.3/28.6/44.8,Denpasar=16.7/28.7/38.3,Denver=-4.1/12.4/35.6,Detroit=-4.2/13.3/36.0,Dhaka=4.7/21.3/34.7,Dikson=-26.6/-9.7/8.9,Dili=8.3/29.2/53.5,Djibouti=8.9/28.0/52.8,Dodoma=-3.4/18.2/40.6,Dolisie=1.5/20.7/41.5,Douala=5.7/27.0/42.9,Dubai=4.8/27.2/43.2,Dublin=-15.7/9.9/18.7,Dunedin=-3.4/11.7/29.3,Durban=6.2/20.9/45.7,Dushanbe=-3.8/13.4/34.0,Edinburgh=-13.5/8.4/20.0,Edmonton=-14.0/5.8/34.7,El Paso=3.1/17.4/32.2,Entebbe=3.1/22.5/42.4,Erbil=3.1/22.4/43.0,Erzurum=-16.4/4.5/22.7,Fairbanks=-18.7/0.7/23.8,Fianarantsoa=-4.3/15.2/30.4,Flores,  Petén=2.7/24.1/43.1,Frankfurt=-5.6/11.0/29.9,Fresno=-4.1/16.0/34.1,Fukuoka=-9.1/16.2/39.4,Gaborone=4.6/22.2/41.3,Gabès=-1.5/15.3/38.0,Gagnoa=4.4/26.2/46.1,Gangtok=-9.7/12.8/31.4,Garissa=13.8/27.2/45.0,Garoua=15.1/28.2/44.7,George Town=11.3/23.0/34.6,Ghanzi=4.3/21.8/40.8,Gjoa Haven=-29.8/-13.6/5.1,Guadalajara=3.3/22.8/38.1,Guangzhou=0.7/21.9/44.5,Guatemala City=1.0/18.3/30.5,Halifax=-13.2/2.6/18.5,Hamburg=-8.6/9.7/24.2,Hamilton=-3.7/12.3/24.6,Hanga Roa=-1.5/18.5/39.5,Hanoi=12.3/28.1/43.1,Harare=2.1/17.1/53.3,Harbin=-13.9/4.0/24.3,Hargeisa=1.8/25.8/45.7,Hat Yai=15.4/28.4/48.1,Havana=2.8/24.8/47.1,Helsinki=-6.0/9.2/25.2,Heraklion=10.8/24.1/42.4,Hiroshima=-4.2/15.2/33.1,Ho Chi Minh City=3.8/27.5/47.9,Hobart=-5.2/10.3/27.8,Hong Kong=3.9/24.1/41.5,Honiara=16.2/28.4/59.7,Honolulu=11.0/27.9/38.1,Houston=13.1/23.8/38.5,Ifrane=-9.5/11.9/28.2,Indianapolis=-6.5/13.1/32.9,Iqaluit=-36.6/-9.3/13.0,Irkutsk=-14.2/4.1/24.0,Istanbul=4.6/18.2/45.5,Jacksonville=2.0/22.0/39.5,Jakarta=9.2/26.9/43.5,Jayapura=2.1/26.5/51.1,Jerusalem=3.4/17.9/36.6,Johannesburg=4.6/16.4/33.5,Jos=6.6/25.9/49.1,Juba=12.5/28.7/51.9,Kabul=-8.7/12.9/36.8,Kampala=-1.6/18.3/43.9,Kandi=11.3/25.3/38.7,Kankan=10.0/26.3/57.0,Kano=6.2/23.3/45.3,Kansas City=-11.0/8.9/22.6,Karachi=5.2/25.6/42.8,Karonga=-7.4/23.9/44.3,Kathmandu=-5.4/19.4/39.7,Khartoum=15.2/30.9/55.3,Kingston=9.3/28.8/49.4,Kinshasa=3.0/23.4/45.6,Kolkata=5.4/27.5/47.1,Kuala Lumpur=11.5/28.5/43.7,Kumasi=16.2/27.0/43.0,Kunming=-8.7/17.2/33.0,Kuopio=-15.5/0.8/23.7,Kuwait City=2.3/23.7/42.6,Kyiv=-14.1/8.5/28.2,Kyoto=-2.5/13.6/31.7,La Ceiba=8.8/25.7/40.6,La Paz=7.6/25.4/42.2,Lagos=13.6/23.5/37.1,Lahore=3.2/23.0/39.2,Lake Havasu City=2.1/22.2/38.6,Lake Tekapo=-10.5/9.5/35.6,Las Palmas de Gran Canaria=0.0/20.9/37.9,Las Vegas=7.1/21.3/39.7,Launceston=-7.0/14.9/36.8,Lhasa=-16.5/5.2/13.7,Libreville=8.0/23.9/43.7,Lisbon=-3.9/15.7/39.8,Livingstone=11.5/24.5/40.1,Ljubljana=-6.8/8.9/23.6,Lodwar=7.8/28.0/40.9,Lomé=7.7/26.6/49.2,London=-10.5/9.3/24.3,Los Angeles=3.3/20.6/39.9,Louisville=-5.0/11.5/30.3,Luanda=9.1/24.5/42.8,Lubumbashi=4.2/22.0/44.4,Lusaka=-7.2/18.3/34.3,Luxembourg City=-8.1/8.9/34.2,Lviv=-14.9/3.6/19.3,Lyon=-10.0/11.0/34.5,Madrid=-2.9/18.2/40.3,Mahajanga=7.8/24.3/47.1,Makassar=-1.7/22.9/43.4,Makurdi=5.1/22.5/40.5,Malabo=9.2/24.6/44.9,Malé=9.3/29.1/42.1,Managua=1.6/26.2/42.3,Manama=1.8/27.7/50.8,Mandalay=13.4/26.2/37.1,Mango=8.2/24.9/44.4,Manila=12.7/29.2/45.5,Maputo=4.7/25.9/55.9,Marrakesh=0.8/19.6/51.0,Marseille=-9.5/15.7/36.9,Maun=-5.7/22.2/38.3,Medan=8.5/28.1/45.8,Mek'ele=7.5/23.6/39.2,Melbourne=-3.1/16.7/36.5,Memphis=-3.4/19.9/50.4,Mexicali=2.9/18.3/32.2,Mexico City=-6.2/19.6/48.8,Miami=4.5/25.3/51.4,Milan=-14.4/12.8/32.9,Milwaukee=-2.1/10.0/32.7,Minneapolis=-10.1/7.1/26.5,Minsk=-12.9/7.6/40.6,Mogadishu=5.0/26.2/44.0,Mombasa=-6.5/21.5/46.8,Monaco=0.8/18.4/32.4,Moncton=-10.4/3.4/19.7,Monterrey=2.5/20.9/39.0,Montreal=-10.7/5.8/28.1,Moscow=-9.0/6.9/30.0,Mumbai=3.8/30.5/51.4,Murmansk=-14.3/2.0/13.7,Muscat=6.8/29.8/55.1,Mzuzu=-0.8/17.2/43.3,N'Djamena=3.8/28.3/49.2,Naha=-3.0/20.7/41.3,Nairobi=-8.9/14.7/38.0,Nakhon Ratchasima=12.6/28.2/41.8,Napier=-1.6/15.9/31.5,Napoli=-7.2/20.9/46.1,Nashville=3.3/17.7/30.8,Nassau=6.3/25.2/42.4,Ndola=1.9/15.7/40.3,New Delhi=3.5/24.5/50.4,New Orleans=3.7/21.8/42.5,New York City=-3.8/13.8/32.2,Ngaoundéré=1.3/17.5/34.4,Niamey=8.5/29.2/47.0,Nicosia=0.2/19.4/34.7,Niigata=-2.3/15.9/47.1,Nouadhibou=3.8/22.4/50.3,Nouakchott=-2.3/24.6/47.3,Novosibirsk=-15.1/-0.5/26.9,Nuuk=-21.2/-0.7/20.0,Odesa=-10.5/9.3/34.9,Odienné=2.1/22.0/34.3,Oklahoma City=-10.6/15.4/30.7,Omaha=-6.7/11.3/31.0,Oranjestad=6.1/31.5/58.9,Oslo=-7.3/11.1/24.4,Ottawa=-7.1/9.1/28.4,Ouagadougou=4.8/29.7/45.8,Ouahigouya=6.3/26.5/42.1,Ouarzazate=-7.6/16.7/35.4,Oulu=-20.7/1.9/20.3,Palembang=13.5/26.9/47.7,Palermo=-1.2/19.4/34.5,Palm Springs=3.6/20.6/39.2,Palmerston North=-8.8/13.1/30.1,Panama City=14.2/29.5/46.6,Parakou=2.9/25.7/39.2,Paris=-5.6/11.5/31.6,Perth=2.4/15.0/39.0,Petropavlovsk-Kamchatsky=-28.0/2.3/21.9,Philadelphia=-4.5/14.7/29.1,Phnom Penh=17.8/30.1/53.3,Phoenix=-2.6/22.5/41.7,Pittsburgh=-5.8/11.2/26.5,Podgorica=-5.0/13.8/26.9,Pointe-Noire=10.2/23.6/35.9,Pontianak=11.3/29.0/58.8,Port Moresby=8.6/25.4/54.1,Port Sudan=8.9/28.4/49.1,Port Vila=7.1/26.1/44.6,Port-Gentil=0.8/26.4/41.3,Portland (OR)=-8.2/10.1/30.1,Porto=-8.9/14.4/42.0,Prague=-14.2/10.9/28.5,Praia=5.0/25.1/38.7,Pretoria=0.8/18.5/35.0,Pyongyang=-13.2/10.0/31.9,Rabat=1.4/15.3/37.1,Rangpur=12.2/25.5/44.9,Reggane=11.1/30.3/57.2,Reykjavík=-19.0/3.3/27.6,Riga=-18.1/2.5/19.2,Riyadh=11.7/28.4/46.4,Rome=-2.3/18.3/38.0,Roseau=11.6/25.4/45.0,Rostov-on-Don=-3.6/9.5/25.8,Sacramento=-1.6/16.3/33.8,Saint Petersburg=-1.1/8.9/25.0,Saint-Pierre=-18.0/6.4/27.6,Salt Lake City=-8.7/10.4/25.2,San Antonio=-0.5/19.2/33.1,San Diego=-4.5/20.5/34.2,San Francisco=-8.6/14.8/51.5,San Jose=-9.3/16.2/30.5,San José=0.8/23.9/43.5,San Juan=10.4/26.5/49.7,San Salvador=5.3/21.8/41.9,Sana'a=-0.5/20.7/33.9,Santo Domingo=7.6/27.9/43.7,Sapporo=-3.0/12.1/26.4,Sarajevo=-5.4/10.5/22.9,Saskatoon=-17.2/-0.6/15.2,Seattle=-18.4/8.7/34.1,Seoul=-22.4/10.0/29.1,Seville=6.6/19.1/32.2,Shanghai=-2.8/14.9/31.9,Singapore=14.0/29.2/47.8,Skopje=-3.1/9.7/32.7,Sochi=-3.0/13.4/32.3,Sofia=-10.6/9.4/29.7,Sokoto=-2.7/28.2/46.0,Split=-13.7/15.8/34.2,St. John's=-23.3/4.6/26.5,St. Louis=-1.5/14.9/32.9,Stockholm=-20.1/5.6/32.9,Surabaya=7.8/27.9/48.1,Suva=8.8/26.8/40.3,Suwałki=-12.9/4.6/27.0,Sydney=2.3/17.2/40.7,Ségou=8.6/29.2/68.2,Tabora=11.4/25.2/46.7,Tabriz=-4.0/11.6/30.0,Taipei=-2.7/21.6/42.5,Tallinn=-17.8/5.2/17.5,Tamale=13.5/27.5/49.8,Tamanrasset=4.9/21.7/40.4,Tampa=9.9/23.7/35.1,Tashkent=0.3/14.8/36.4,Tauranga=-7.7/16.6/36.7,Tbilisi=-5.8/14.2/33.9,Tegucigalpa=7.8/22.5/40.2,Tehran=-4.6/20.2/37.4,Tel Aviv=0.4/17.8/30.3,Thessaloniki=2.9/16.3/40.8,Thiès=6.3/24.7/45.6,Tijuana=-5.6/19.7/46.0,Timbuktu=9.7/30.0/47.9,Tirana=-6.0/11.8/29.0,Toamasina=2.4/21.5/46.7,Tokyo=0.4/17.4/34.5,Toliara=6.5/24.9/40.6,Toluca=-4.5/11.8/30.2,Toronto=-12.9/9.2/35.5,Tripoli=-5.4/21.8/49.1,Tromsø=-9.4/-1.3/10.5,Tucson=0.3/21.4/42.3,Tunis=7.4/19.4/36.2,Ulaanbaatar=-22.4/-2.0/16.0,Upington=0.9/19.0/42.3,Vaduz=-12.0/12.0/28.6,Valencia=-8.2/13.3/35.8,Valletta=-13.8/18.3/42.6,Vancouver=-21.2/9.2/33.8,Veracruz=3.8/24.5/40.7,Vienna=-3.0/6.1/21.2,Vientiane=11.7/27.5/44.5,Villahermosa=13.3/27.9/53.4,Vilnius=-17.1/3.6/24.0,Virginia Beach=-6.4/15.2/35.2,Vladivostok=-15.3/7.5/25.9,Warsaw=-11.6/6.9/32.8,Washington, D.C.=-8.9/12.8/25.3,Wau=9.5/30.5/57.7,Wellington=-7.8/12.4/23.8,Whitehorse=-18.9/-6.1/11.1,Wichita=-6.2/12.7/35.5,Willemstad=12.3/25.6/42.3,Winnipeg=-12.6/5.9/20.7,Wrocław=-12.6/12.9/32.5,Xi'an=-4.6/14.0/27.1,Yakutsk=-24.3/-9.9/16.6,Yangon=9.4/29.4/51.1,Yaoundé=-1.5/22.9/43.8,Yellowknife=-28.2/-6.4/6.1,Yerevan=-9.5/12.2/32.0,Yinchuan=-12.9/7.9/29.0,Zagreb=-14.7/11.1/25.9,Zanzibar City=7.5/24.4/48.3,Zürich=-15.8/8.0/37.5,Ürümqi=-10.4/8.1/28.0,İzmir=7.4/20.0/39.4}\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file, err := os.Open(tt.args.filePath)
			if err != nil {
				t.Error(err)
			}
			p := V3{}
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

func BenchmarkProcessV3(b *testing.B) {
	file, err := os.Open("./testdata/measurements.txt")
	if err != nil {
		b.Error(err)
	}

	defer file.Close()

	var out bytes.Buffer

	for i := 0; i < b.N; i++ {
		p := V3{}
		out := &bytes.Buffer{}
		err = p.Process(file, out)
		if err != nil {
			b.Error(err)
		}
	}

	_, _ = fmt.Fprintf(io.Discard, "%s", out.String())
}
